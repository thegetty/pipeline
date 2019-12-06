import re
import os
import json
import sys
import warnings
import uuid
import csv
import pprint
import pathlib
import itertools
import datetime
from collections import Counter, defaultdict, namedtuple
from contextlib import suppress
import inspect
import urllib.parse

import time
import timeit
from sqlalchemy import create_engine
import dateutil.parser

import graphviz
import bonobo
from bonobo.config import use, Option, Service, Configurable
from bonobo.nodes import Limit
from bonobo.constants import NOT_MODIFIED

import settings

from cromulent import model, vocab
from cromulent.model import factory
from cromulent.extract import extract_physical_dimensions, extract_monetary_amount

import pipeline.execution
from pipeline.projects import PipelineBase
from pipeline.util import \
			CaseFoldingSet, \
			RecursiveExtractKeyedValue, \
			ExtractKeyedValue, \
			ExtractKeyedValues, \
			MatchingFiles, \
			identity, \
			implode_date, \
			timespan_before, \
			timespan_after, \
			replace_key_pattern, \
			strip_key_prefix, \
			timespan_from_outer_bounds
from pipeline.util.cleaners import \
			parse_location, \
			parse_location_name, \
			date_parse, \
			date_cleaner
from pipeline.io.file import MergingFileWriter
from pipeline.io.memory import MergingMemoryWriter
# from pipeline.io.arches import ArchesWriter
from pipeline.linkedart import \
			add_crom_data, \
			get_crom_object, \
			MakeLinkedArtRecord, \
			MakeLinkedArtLinguisticObject, \
			MakeLinkedArtHumanMadeObject, \
			MakeLinkedArtAuctionHouseOrganization, \
			MakeLinkedArtOrganization, \
			MakeLinkedArtPerson, \
			make_la_place
from pipeline.io.csv import CurriedCSVReader
from pipeline.nodes.basic import \
			AddFieldNames, \
			GroupRepeatingKeys, \
			GroupKeys, \
			AddArchesModel, \
			Serializer, \
			Trace
from pipeline.util.rewriting import rewrite_output_files, JSONValueRewriter

PROJECT_NAME = "knoedler"
UID_TAG_PREFIX = f'tag:getty.edu,2019:digital:pipeline:{PROJECT_NAME}:REPLACE-WITH-UUID#'



# TODO: copied from provenance.util; refactor
def filter_empty_person(data: dict, _):
	'''
	If all the values of the supplied dictionary are false (or false after int conversion
	for keys ending with 'ulan'), return `None`. Otherwise return the dictionary.
	'''
	set_flags = []
	for k, v in data.items():
		if k.endswith('ulan'):
			if v in ('', '0'):
				s = False
			else:
				s = True
		elif k in ('pi_record_no', 'star_rec_no'):
			s = False
		else:
			s = bool(v)
		set_flags.append(s)
	if any(set_flags):
		return data
	else:
		return None

def knoedler_uri(*values):
	'''Convert a set of identifying `values` into a URI'''
	if values:
		suffix = ','.join([urllib.parse.quote(str(v)) for v in values])
		return UID_TAG_PREFIX + suffix
	else:
		suffix = str(uuid.uuid4())
		return UID_TAG_PREFIX + suffix

def record_id(data):
	book = data['stock_book_no']
	page = data['page_number']
	row = data['row_number']
	return (book, page, row)

def add_person_uri(data:dict):
	# TODO: move this into MakeLinkedArtPerson
	auth_name = data.get('authority')
	if data.get('ulan'):
		ulan = data['ulan']
		key = f'PERSON-ULAN-{ulan}'
		data['uri'] = knoedler_uri('PERSON', 'ULAN', ulan)
		data['ulan'] = ulan
	elif auth_name and '[' not in auth_name:
		data['uri'] = knoedler_uri('PERSON', 'AUTHNAME', auth_name)
		data['identifiers'] = [
			vocab.PrimaryName(ident='', content=auth_name)
		]
	else:
		# not enough information to identify this person uniquely, so use the source location in the input file
		data['uri'] = knoedler_uri('PERSON', 'PI_REC_NO', data['parent_data']['pi_record_no'])

	return data

def add_group_uri(data:dict):
	# TODO: move this into MakeLinkedArtOrganization
	auth_name = data.get('authority')
	if data.get('ulan'):
		ulan = data['ulan']
		key = f'GROUP-ULAN-{ulan}'
		data['uri'] = knoedler_uri('GROUP', 'ULAN', ulan)
		data['ulan'] = ulan
	elif auth_name and '[' not in auth_name:
		data['uri'] = knoedler_uri('GROUP', 'AUTHNAME', auth_name)
		data['identifiers'] = [
			vocab.PrimaryName(ident='', content=auth_name)
		]
	else:
		# not enough information to identify this person uniquely, so use the source location in the input file
		data['uri'] = knoedler_uri('GROUP', 'PI_REC_NO', data['parent_data']['pi_record_no'])

	return data

def knoedler_number_id(content, static_instances):
	k_id = vocab.LocalNumber(ident='', content=content)
	assignment = model.AttributeAssignment(ident='')
	assignment.carried_out_by = static_instances.get_instance('Group', 'knoedler')
	k_id.assigned_by = assignment
	return k_id

class AddBook(Configurable):
	make_la_lo = Service('make_la_lo')
	make_la_hmo = Service('make_la_hmo')
	static_instances = Option(default="static_instances")
	
	def __call__(self, data:dict, make_la_lo, make_la_hmo):
		book = data['book_record']
		book_id, _, _ = record_id(book)
		data['_physical_book'] = {
			'uri': knoedler_uri('Book', book_id),
			'object_type': vocab.Book,
			'label': f'Knoedler Stock Book {book_id}',
			'identifiers': [knoedler_number_id(book_id, self.static_instances)],
		}
		make_la_hmo(data['_physical_book'])

		data['_text_book'] = {
			'uri': knoedler_uri('Text', 'Book', book_id),
			'object_type': vocab.AccountBookText,
			'label': f'Knoedler Stock Book {book_id}',
			'identifiers': [knoedler_number_id(book_id, self.static_instances)],
			'carried_by': [data['_physical_book']]
		}
		make_la_lo(data['_text_book'])

		return data

class AddPage(Configurable):
	make_la_lo = Service('make_la_lo')
	make_la_hmo = Service('make_la_hmo')
	static_instances = Option(default="static_instances")
	
	def __call__(self, data:dict, make_la_lo, make_la_hmo):
		book = data['book_record']
		book_id, page_id, _ = record_id(book)

		d = vocab.SequencePosition()
		d.value = page_id
		d.unit = vocab.instances['numbers']

		data['_physical_page'] = {
			'uri': knoedler_uri('Book', book_id, 'Page', page_id),
			'object_type': vocab.Page,
			'label': f'Knoedler Stock Book {book_id}, Page {page_id}',
			'identifiers': [knoedler_number_id(book_id, self.static_instances)],
			'part_of': [data['_physical_book']],
		}
		make_la_hmo(data['_physical_page'])
	
		data['_text_page'] = {
			'uri': knoedler_uri('Text', 'Book', book_id, 'Page', page_id),
			'object_type': vocab.AccountBookText,
			'label': f'Knoedler Stock Book {book_id}, Page {page_id}',
			'identifiers': [(page_id, vocab.LocalNumber(ident=''))],
			'part_of': [data['_text_book']],
			'carried_by': [data['_physical_page']],
			'dimensions': [d] # TODO: add dimension handling to MakeLinkedArtLinguisticObject
		}
		if data.get('heading'):
			# This is a transcription of the heading of the page
			# Meaning it is part of the page linguistic object
			data['_text_page']['heading'] = data['heading'] # TODO: add heading handling to MakeLinkedArtLinguisticObject
		if data.get('subheading'):
			# Transcription of the subheading of the page
			data['_text_page']['subheading'] = data['subheading'] # TODO: add subheading handling to MakeLinkedArtLinguisticObject
		make_la_lo(data['_text_page'])

		return data

class AddRow(Configurable):
	make_la_lo = Service('make_la_lo')
	static_instances = Option(default="static_instances")
	
	def __call__(self, data:dict, make_la_lo):
		book = data['book_record']
		book_id, page_id, row_id = record_id(book)

		d = vocab.SequencePosition()
		d.value = row_id
		d.unit = vocab.instances['numbers']
	
		notes = []
		# TODO: add attributed star record number to row as a LocalNumber
		for k in ('description', 'working_note', 'verbatim_notes'):
			if book.get(k):
				notes.append(vocab.Note(ident='', content=book[k]))

		data['_text_row'] = {
			'uri': knoedler_uri('Text', 'Book', book_id, 'Page', page_id, 'Row', row_id),
			'label': f'Knoedler Stock Book {book_id}, Page {page_id}, Row {row_id}',
			'identifiers': [(row_id, vocab.LocalNumber(ident=''))],
			'part_of': [data['_text_page']],
			'dimensions': [d], # TODO: add dimension handling to MakeLinkedArtLinguisticObject
			'referred_to_by': notes,
		}
		make_la_lo(data['_text_row'])
	
		return data

@use('vocab_type_map')
@use('make_la_org')
def add_object(data: dict, make_la_org, vocab_type_map):
	odata = data['object']
	title = odata['title']
	typestring = odata.get('object_type', '')

	try:
		uri = knoedler_uri('Object', odata['knoedler_number'])
	except:
		uri = knoedler_uri('Object', 'Internal', data['pi_record_no'])
	data['_object'] = {
		'uri': uri,
		'title': title,
		'identifiers': [],
	}
	if typestring in vocab_type_map:
		clsname = vocab_type_map.get(typestring, None)
		otype = getattr(vocab, clsname)
		data['_object']['object_type'] = otype
	else:
		data['_object']['object_type'] = model.HumanMadeObject

	consigner = data['consigner']
	if consigner:
		add_group_uri(consigner)
		make_la_org(consigner)
		consigner_num = consigner['no']
		consigner_id = vocab.LocalNumber(ident='', label=f'Consigned number: {consigner_num}', content=consigner_num)
		assignment = model.AttributeAssignment(ident='')
		assignment.carried_out_by = get_crom_object(consigner)
		consigner_id.assigned_by = assignment
		data['_object']['identifiers'].append(consigner_id)
		data['_consigner'] = consigner

	return data

def transaction_switch(data: dict):
	return data


#mark - Knoedler Pipeline class

class KnoedlerPipeline(PipelineBase):
	'''Bonobo-based pipeline for transforming Knoedler data from CSV into JSON-LD.'''
	def __init__(self, input_path, data, **kwargs):
		self.uid_tag_prefix = UID_TAG_PREFIX
		self.project_name = 'knoedler'

		super().__init__()
		self.graph = None
		self.models = kwargs.get('models', settings.arches_models)
		self.header_file = data['header_file']
		self.files_pattern = data['files_pattern']
		self.limit = kwargs.get('limit')
		self.debug = kwargs.get('debug', False)
		self.input_path = input_path

		fs = bonobo.open_fs(input_path)
		with fs.open(self.header_file, newline='') as csvfile:
			r = csv.reader(csvfile)
			self.headers = [v.lower() for v in next(r)]

	def setup_static_instances(self):
		instances = super().setup_static_instances()

		knoedler_ulan = 500304270
		knoedler_name = 'M. Knoedler & Co.'
		KNOEDLER_URI = self.make_uri('ORGANIZATION', 'ULAN', str(knoedler_ulan))
		knoedler = model.Group(ident=KNOEDLER_URI, label=knoedler_name)
		knoedler.identified_by = vocab.PrimaryName(ident='', content=knoedler_name)
		knoedler.exact_match = model.BaseResource(ident=f'http://vocab.getty.edu/ulan/{knoedler_ulan}')
		
		instances['Group'].update({
			'knoedler': knoedler
		})
		return instances

	# Set up environment
	def get_services(self):
		'''Return a `dict` of named services available to the bonobo pipeline.'''
		services = super().get_services()
		services.update({
			# to avoid constructing new MakeLinkedArtPerson objects millions of times, this
			# is passed around as a service to the functions and classes that require it.
			'make_la_person': MakeLinkedArtPerson(),
			'make_la_lo': MakeLinkedArtLinguisticObject(),
			'make_la_hmo': MakeLinkedArtHumanMadeObject(),
			'make_la_org': MakeLinkedArtOrganization(),
		})
		return services

	def add_sales_chain(self, graph, records, serialize=True):
		'''Add transformation of sales records to the bonobo pipeline.'''
		sales_records = graph.add_chain(
# 			"star_record_no",
# 			"pi_record_no",
			GroupRepeatingKeys(
				drop_empty=True,
				mapping={
					'_artists': {
						'postprocess': [
							filter_empty_person,
							lambda x, _: strip_key_prefix('artist_', x),
						],
						'prefixes': (
							"artist_name",
							"artist_authority",
							"artist_nationality",
							"artist_attribution_mod",
							"artist_attribution_mod_auth",
							"star_rec_no",
							"artist_ulan")},
					'purchase_seller': {
						'postprocess': [
							filter_empty_person,
							lambda x, _: strip_key_prefix('purchase_seller_', x),
						],
						'prefixes': (
							"purchase_seller_name",
							"purchase_seller_loc",
							"purchase_seller_auth_name",
							"purchase_seller_auth_loc",
							"purchase_seller_auth_mod",
							"purchase_seller_ulan",
						)
					},
					'purchase_buyer': {
						'postprocess': [
							filter_empty_person,
							lambda x, _: strip_key_prefix('purchase_buyer_', x),
						],
						'prefixes': (
							"purchase_buyer_own",
							"purchase_buyer_share",
							"purchase_buyer_ulan",
						)
					},
					'prev_own': {
						'prefixes': (
							"prev_own",
							"prev_own_loc",
							"prev_own_ulan",
						)
					},
					'sale_buyer': {
						'prefixes': (
							"sale_buyer_name",
							"sale_buyer_loc",
							"sale_buyer_mod",
							"sale_buyer_auth_name",
							"sale_buyer_auth_addr",
							"sale_buyer_auth_mod",
							"sale_buyer_ulan",
						)
					}
				}
			),
			GroupKeys(mapping={
				'present_location': {
					'postprocess': lambda x, _: strip_key_prefix('present_loc_', x),
					'properties': (
						"present_loc_geog",
						"present_loc_inst",
						"present_loc_acc",
						"present_loc_note",
						"present_loc_ulan",
					)
				},
				'consigner': {
					'postprocess': lambda x, _: strip_key_prefix('consign_', x),
					'properties': (
						"consign_no",
						"consign_name",
						"consign_loc",
						"consign_ulan",
					)
				},
				'object': {
					'properties': (
						"knoedler_number",
						"title",
						"subject",
						"genre",
						"object_type",
						"materials",
						"dimensions",
					)
				},
				'sale_date': {
					'postprocess': lambda x, _: strip_key_prefix('sale_date_', x),
					'properties': (
						"sale_date_year",
						"sale_date_month",
						"sale_date_day",
					)
				},
				'entry_date': {
					'postprocess': lambda x, _: strip_key_prefix('entry_date_', x),
					'properties': (
						"entry_date_year",
						"entry_date_month",
						"entry_date_day",
					)
				},
				'purchase': {
					'properties': (
						"purch_amount",
						"purch_currency",
						"purch_note",
					)
				},
				'knoedler_purchase': {
					'properties': (
						"knoedpurch_amt",
						"knoedpurch_curr",
						"knoedpurch_note",
					)
				},
				'knoedler_share': {
					'properties': (
						"knoedshare_amt",
						"knoedshare_curr",
						"knoedshare_note",
					)
				},
				'price': {
					'properties': (
						"price_amount",
						"price_currency",
						"price_note",
					)
				},
				'book_record': {
					'properties': (
						"stock_book_no",
						"page_number",
						"row_number",
						"description",
						"folio",
						"link",
						"heading",
						"subheading",
						"verbatim_notes",
						"working_note",
						"transaction",
					)
				},
				'post_owner': {
					'properties': (
						"post_owner",
						"post_owner_ulan",
					)
				}
			}),
			_input=records.output
		)
		
		books = self.add_book_chain(graph, sales_records)
		pages = self.add_page_chain(graph, books)
		rows = self.add_row_chain(graph, pages)
		objects = self.add_object_chain(graph, rows)

		tx = graph.add_chain(
			transaction_switch,
			_input=objects.output
		)			

# 		if serialize:
# 			self.add_serialization_chain(graph, books.output, model=self.models['Activity'])
		return tx

	def add_book_chain(self, graph, sales_records, serialize=True):
		books = graph.add_chain(
# 			add_book,
			AddBook(static_instances=self.static_instances),
			_input=sales_records.output
		)
		phys = graph.add_chain(
			ExtractKeyedValue(key='_physical_book'),
			_input=books.output
		)
		text = graph.add_chain(
			ExtractKeyedValue(key='_text_book'),
			_input=books.output
		)
		if serialize:
			self.add_serialization_chain(graph, phys.output, model=self.models['HumanMadeObject'])
			self.add_serialization_chain(graph, text.output, model=self.models['LinguisticObject'])
		return books

	def add_page_chain(self, graph, books, serialize=True):
		pages = graph.add_chain(
			AddPage(static_instances=self.static_instances),
			_input=books.output
		)
		phys = graph.add_chain(
			ExtractKeyedValue(key='_physical_page'),
			_input=pages.output
		)
		text = graph.add_chain(
			ExtractKeyedValue(key='_text_page'),
			_input=pages.output
		)
		if serialize:
			self.add_serialization_chain(graph, phys.output, model=self.models['HumanMadeObject'])
			self.add_serialization_chain(graph, text.output, model=self.models['LinguisticObject'])
		return pages

	def add_row_chain(self, graph, pages, serialize=True):
		rows = graph.add_chain(
			AddRow(static_instances=self.static_instances),
			_input=pages.output
		)
		text = graph.add_chain(
			ExtractKeyedValue(key='_text_row'),
			_input=rows.output
		)
		if serialize:
			self.add_serialization_chain(graph, text.output, model=self.models['LinguisticObject'])
		return rows

	def add_object_chain(self, graph, rows, serialize=True):
		objects = graph.add_chain(
			add_object,
			_input=rows.output
		)
		hmos = graph.add_chain(
			ExtractKeyedValue(key='_object'),
			MakeLinkedArtHumanMadeObject(),
			_input=objects.output
		)
		consigners = graph.add_chain(
			ExtractKeyedValue(key='_consigner'),
			_input=objects.output
		)
		artists = graph.add_chain(
			ExtractKeyedValues(key='_artists'),
			add_person_uri,
			MakeLinkedArtPerson(),
			_input=objects.output
		)
		if serialize:
			self.add_serialization_chain(graph, hmos.output, model=self.models['HumanMadeObject'])
			self.add_serialization_chain(graph, consigners.output, model=self.models['Group'])
			self.add_serialization_chain(graph, artists.output, model=self.models['Person'])
		return objects

	def _construct_graph(self):
		'''
		Construct bonobo.Graph object(s) for the entire pipeline.
		'''
		g = bonobo.Graph()

		contents_records = g.add_chain(
			MatchingFiles(path='/', pattern=self.files_pattern, fs='fs.data.knoedler'),
			CurriedCSVReader(fs='fs.data.knoedler', limit=self.limit),
			AddFieldNames(field_names=self.headers),
		)
		sales = self.add_sales_chain(g, contents_records, serialize=True)

		self.graph = g

	def get_graph(self):
		'''Return a single bonobo.Graph object for the entire pipeline.'''
		if not self.graph:
			self._construct_graph()

		return self.graph

	def run(self, services=None, **options):
		'''Run the Knoedler bonobo pipeline.'''
		print(f'- Limiting to {self.limit} records per file', file=sys.stderr)
		if not services:
			services = self.get_services(**options)

		print('Running graph...', file=sys.stderr)
		graph = self.get_graph(**options)
		self.run_graph(graph, services=services)


class KnoedlerFilePipeline(KnoedlerPipeline):
	'''
	Knoedler pipeline with serialization to files based on Arches model and resource UUID.

	If in `debug` mode, JSON serialization will use pretty-printing. Otherwise,
	serialization will be compact.
	'''
	def __init__(self, input_path, data, **kwargs):
		super().__init__(input_path, data, **kwargs)
		self.writers = []
		self.output_path = kwargs.get('output_path')

	def serializer_nodes_for_model(self, *args, model=None, use_memory_writer=True, **kwargs):
		nodes = []
		if self.debug:
			if use_memory_writer:
				w = MergingMemoryWriter(directory=self.output_path, partition_directories=True, compact=False, model=model)
			else:
				w = MergingFileWriter(directory=self.output_path, partition_directories=True, compact=False, model=model)
			nodes.append(w)
		else:
			if use_memory_writer:
				w = MergingMemoryWriter(directory=self.output_path, partition_directories=True, compact=True, model=model)
			else:
				w = MergingFileWriter(directory=self.output_path, partition_directories=True, compact=True, model=model)
			nodes.append(w)
		self.writers += nodes
		return nodes

	def run(self, **options):
		'''Run the Knoedler bonobo pipeline.'''
		start = timeit.default_timer()
		services = self.get_services(**options)
		super().run(services=services, **options)
		print(f'Pipeline runtime: {timeit.default_timer() - start}', file=sys.stderr)

		count = len(self.writers)
		for seq_no, w in enumerate(self.writers):
			print('[%d/%d] writers being flushed' % (seq_no+1, count))
			if isinstance(w, MergingMemoryWriter):
				w.flush()

		print('====================================================')
		print('Total runtime: ', timeit.default_timer() - start)
