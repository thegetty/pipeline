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



#mark - Knoedler Pipeline class

class KnoedlerPipeline(PipelineBase):
	'''Bonobo-based pipeline for transforming Knoedler data from CSV into JSON-LD.'''
	def __init__(self, input_path, data, **kwargs):
		super().__init__()
		self.project_name = 'knoedler'
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

	# Set up environment
	def get_services(self):
		'''Return a `dict` of named services available to the bonobo pipeline.'''
		services = super().get_services()
		services.update({
			# to avoid constructing new MakeLinkedArtPerson objects millions of times, this
			# is passed around as a service to the functions and classes that require it.
			'make_la_person': MakeLinkedArtPerson(),
		})
		return services

	def add_sales_chain(self, graph, records, serialize=True):
		'''Add transformation of sales records to the bonobo pipeline.'''
		sales = graph.add_chain(
			GroupRepeatingKeys(
				drop_empty=True,
				mapping={
					'_artists': {
						'postprocess': filter_empty_person,
						'prefixes': (
							"artist_name",
							"artist_authority",
							"artist_nationality",
							"artist_attribution_mod",
							"artist_attribution_mod_auth",
							"star_rec_no",
							"artist_ulan")},
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
				}
			}),
# 			"star_record_no
# 			"pi_record_no"
# 			"stock_book_no
# 			"knoedler_number"
# 			"page_number
# 			"row_number
# 			"consign_no
# 			"consign_name"
# 			"consign_loc"
# 			"consign_ulan"
# 			"title"
# 			"description"
# 			"subject"
# 			"genre"
# 			"object_type"
# 			"materials"
# 			"dimensions"
# 			"entry_date_year
# 			"entry_date_month
# 			"entry_date_day
# 			"sale_date_year
# 			"sale_date_month
# 			"sale_date_day
# 			"purch_amount"
# 			"purch_currency"
# 			"purch_note"
# 			"knoedpurch_amt"
# 			"knoedpurch_curr"
# 			"knoedpurch_note"
# 			"price_amount"
# 			"price_currency"
# 			"price_note"
# 			"knoedshare_amt"
# 			"knoedshare_curr"
# 			"knoedshare_note"
# 			"purchase_seller_name_1"
# 			"purchase_seller_loc_1"
# 			"purchase_seller_auth_name_1"
# 			"purchase_seller_auth_loc_1"
# 			"purchase_seller_auth_mod_1"
# 			"purchase_seller_ulan_1"
# 			"purchase_seller_name_2"
# 			"purchase_seller_loc_2"
# 			"purchase_seller_auth_name_2"
# 			"purchase_seller_auth_loc_2"
# 			"purchase_seller_auth_mod_2"
# 			"purchase_seller_ulan_2"
# 			"purchase_buyer_own_1"
# 			"purchase_buyer_share_1"
# 			"purchase_buyer_ulan_1"
# 			"purchase_buyer_own_2"
# 			"purchase_buyer_share_2"
# 			"purchase_buyer_ulan_2"
# 			"purchase_buyer_own_3"
# 			"purchase_buyer_share_3"
# 			"purchase_buyer_ulan_3"
# 			"transaction"
# 			"sale_buyer_name_1"
# 			"sale_buyer_loc_1"
# 			"sale_buyer_mod_1"
# 			"sale_buyer_auth_name_1"
# 			"sale_buyer_auth_addr_1"
# 			"sale_buyer_auth_mod_1"
# 			"sale_buyer_ulan_1"
# 			"sale_buyer_name_2"
# 			"sale_buyer_loc_2"
# 			"sale_buyer_mod_2"
# 			"sale_buyer_auth_name_2"
# 			"sale_buyer_auth_addr_2"
# 			"sale_buyer_auth_mod_2"
# 			"sale_buyer_ulan_2"
# 			"folio"
# 			"prev_own_1"
# 			"prev_own_loc_1"
# 			"prev_own_ulan_1"
# 			"prev_own_2"
# 			"prev_own_loc_2"
# 			"prev_own_ulan_2"
# 			"prev_own_3"
# 			"prev_own_loc_3"
# 			"prev_own_ulan_3"
# 			"prev_own_4"
# 			"prev_own_loc_4"
# 			"prev_own_ulan_4"
# 			"prev_own_5"
# 			"prev_own_loc_5"
# 			"prev_own_ulan_5"
# 			"prev_own_6"
# 			"prev_own_loc_6"
# 			"prev_own_ulan_6"
# 			"prev_own_7"
# 			"prev_own_loc_7"
# 			"prev_own_ulan_7"
# 			"prev_own_8"
# 			"prev_own_loc_8"
# 			"prev_own_ulan_8"
# 			"prev_own_9"
# 			"prev_own_loc_9"
# 			"prev_own_ulan_9"
# 			"post_owner"
# 			"post_owner_ulan"
# 			"working_note"
# 			"verbatim_notes"
# 			"link"
# 			"main_heading"
# 			"subheading"
			Trace(name='foo'),
			_input=records.output
		)
# 		if serialize:
# 			self.add_serialization_chain(graph, sales.output, model=self.models['Activity'])
		return sales

	def add_places_chain(self, graph, auction_events, serialize=True):
		'''Add extraction and serialization of locations.'''
		places = graph.add_chain(
			ExtractKeyedValues(key='_locations'),
			RecursiveExtractKeyedValue(key='part_of'),
			_input=auction_events.output
		)
		if serialize:
			# write OBJECTS data
			self.add_serialization_chain(graph, places.output, model=self.models['Place'])
		return places

	def add_auction_houses_chain(self, graph, auction_events, serialize=True):
		'''Add modeling of the auction houses related to an auction event.'''
		houses = graph.add_chain(
			ExtractKeyedValues(key='auction_house'),
			MakeLinkedArtAuctionHouseOrganization(),
			_input=auction_events.output
		)
		if serialize:
			# write OBJECTS data
			self.add_serialization_chain(graph, houses.output, model=self.models['Group'])
		return houses

	def add_visual_item_chain(self, graph, objects, serialize=True):
		'''Add transformation of visual items to the bonobo pipeline.'''
		items = graph.add_chain(
			ExtractKeyedValue(key='_visual_item'),
			MakeLinkedArtRecord(),
			_input=objects.output
		)
		if serialize:
			# write VISUAL ITEMS data
			self.add_serialization_chain(graph, items.output, model=self.models['VisualItem'], use_memory_writer=False)
		return items

	def add_record_text_chain(self, graph, objects, serialize=True):
		'''Add transformation of record texts to the bonobo pipeline.'''
		texts = graph.add_chain(
			ExtractKeyedValue(key='_sale_record_text'),
			MakeLinkedArtLinguisticObject(),
			_input=objects.output
		)
		if serialize:
			# write RECORD data
			self.add_serialization_chain(graph, texts.output, model=self.models['LinguisticObject'])
		return texts

	def add_people_chain(self, graph, objects, serialize=True):
		'''Add transformation of artists records to the bonobo pipeline.'''
		people = graph.add_chain(
			ExtractKeyedValues(key='_artists'),
			_input=objects.output
		)
		if serialize:
			# write PEOPLE data
			self.add_serialization_chain(graph, people.output, model=self.models['Person'])
		return people

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
