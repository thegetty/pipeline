import re
import os
import json
import sys
import warnings
from fractions import Fraction
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
from cromulent.extract import extract_monetary_amount

from pipeline.projects import PipelineBase, UtilityHelper, PersonIdentity
from pipeline.util import \
			traverse_static_place_instances, \
			truncate_with_ellipsis, \
			implode_date, \
			timespan_from_outer_bounds, \
			GraphListSource, \
			CaseFoldingSet, \
			RecursiveExtractKeyedValue, \
			ExtractKeyedValue, \
			ExtractKeyedValues, \
			MatchingFiles, \
			strip_key_prefix, \
			rename_keys, \
			filter_empty_person, \
			associate_with_tgn_record
from pipeline.util.cleaners import \
			parse_location_name, \
			date_cleaner
from pipeline.io.file import MergingFileWriter
from pipeline.io.memory import MergingMemoryWriter
# from pipeline.io.arches import ArchesWriter
import pipeline.linkedart
from pipeline.linkedart import \
			add_crom_data, \
			get_crom_object, \
			MakeLinkedArtRecord, \
			MakeLinkedArtLinguisticObject, \
			MakeLinkedArtHumanMadeObject, \
			MakeLinkedArtAuctionHouseOrganization, \
			MakeLinkedArtOrganization, \
			MakeLinkedArtPerson, \
			make_la_place, \
			make_tgn_place
from pipeline.io.csv import CurriedCSVReader
from pipeline.nodes.basic import \
			RecordCounter, \
			KeyManagement, \
			PreserveCSVFields, \
			AddArchesModel, \
			Serializer, \
			Trace
from pipeline.util.rewriting import rewrite_output_files, JSONValueRewriter
from pipeline.provenance import ProvenanceBase

class KnoedlerPersonIdentity(PersonIdentity):
	pass

class SharedUtilityHelper(UtilityHelper):
	'''
	Knoedler specific originated code for accessing and interpreting sales data,
	which however can may need to be used by knoedler like datasets, like goupil.
	'''
	def stock_number_identifier(self, data, date, stock_data_key = 'knoedler_number'):
		stock_number = data.get(stock_data_key)
		if stock_number:
			ident = f'Stock Number {stock_number}'
		else:
			pi_num = data['pi_record_no']
			ident = f'[GRI Number {pi_num}]'

		if date:
			ident += f' ({date})'
		return ident

	def transaction_uri_for_record(self, data, incoming=False):
		'''
		Return a URI representing the prov entry which the object (identified by the
		supplied data) is a part of. This may identify just the object being bought or
		sold or, in the case of multiple objects being bought for a single price, a
		single prov entry that encompasses multiple object acquisitions.
		'''
		tx_key = self.transaction_key_for_record(data, incoming)
		return self.make_proj_uri(*tx_key)

	def copy_source_information(self, dst: dict, src: dict):
		if not dst or not isinstance(dst, dict):
			return dst
		for k in self.csv_source_columns:
			with suppress(KeyError):
				dst[k] = src[k]
		return dst

	def make_object_uri(self, pi_rec_no, *uri_key):
		uri_key = list(uri_key)
		same_objects = self.services['same_objects_map']
		different_objects = self.services['different_objects']
		kn = uri_key[-1]
		if kn in different_objects:
			uri_key = uri_key[:-1] + ['flag-separated', kn, pi_rec_no]
		elif kn in same_objects:
			uri_key[-1] = same_objects[uri_key[-1]]
		uri = self.make_proj_uri(*uri_key)
		return uri
			
class KnoedlerUtilityHelper(SharedUtilityHelper):
	'''
	Project-specific code for accessing and interpreting sales data.
	'''
	def __init__(self, project_name, static_instances=None):
		super().__init__(project_name)
		self.person_identity = KnoedlerPersonIdentity(make_shared_uri=self.make_shared_uri, make_proj_uri=self.make_proj_uri)
		self.static_instances = static_instances
		self.csv_source_columns = ['pi_record_no']
		self.make_la_person = MakeLinkedArtPerson()
		self.title_re = re.compile(r'\["(.*)" (title )?(info )?from ([^]]+)\]')
		self.title_ref_re = re.compile(r'Sales Book (\d+), (\d+-\d+), f.(\d+)')
		self.uid_tag_prefix = self.proj_prefix

	def stock_number_identifier(self, data, date):
		return super().stock_number_identifier(data,date)

	def add_person(self, data, record, relative_id, **kwargs):
		self.person_identity.add_uri(data, record_id=relative_id)
		key = data['uri_keys']
		if key in self.services['people_groups']:
			warnings.warn(f'*** TODO: model person record as a GROUP: {pprint.pformat(key)}')
		person = super().add_person(data, record=record, relative_id=relative_id, **kwargs)
		if record:
			person.referred_to_by = record
		return person

	def make_place(self, *args, sales_record=None, **kwargs):
		'''
		Add a reference to the sales record in places that are modeled.
		This will only add references to the most-specific place being modeled,
		leaving the 'part_of' hierarchy remain un-referenced.
		'''
		data = super().make_place(*args, **kwargs)
		if sales_record:
			p = get_crom_object(data)
			p.referred_to_by = sales_record
		return data

	def title_value(self, title):
		if not isinstance(title, str):
			return

		m = self.title_re.search(title)
		if m:
			return m.group(1)
		return title

	def add_title_reference(self, data, title):
		'''
		If the title matches the pattern indicating it was added by an editor and has
		and associated source reference, return a `model.LinguisticObject` for that
		reference.
		
		If the reference can be modeled as a hierarchy of folios and books, that data
		is added to the arrays in the `_physical_objects` and `_linguistic_objects` keys
		of the `data` dict parameter.
		'''
		if not isinstance(title, str):
			return None

		m = self.title_re.search(title)
		if m:
			ref_text = m.group(4)
			ref_match = self.title_ref_re.search(ref_text)
			if ref_match:
				book = ref_match.group(1)
				folio = ref_match.group(3)
				s_uri = self.make_proj_uri('SalesBook', book)
				f_uri = self.make_proj_uri('SalesBook', book, 'Folio', folio)
				
				s_text = vocab.SalesCatalogText(ident=s_uri + '-Text', label=f'Knoedler Sales Book {book}')
				s_hmo = vocab.SalesCatalog(ident=s_uri, label=f'Knoedler Sales Book {book}')
				f_text = vocab.FolioText(ident=f_uri + '-Text', label=f'Knoedler Sales Book {book}, Folio {folio}')
				f_hmo = vocab.Folio(ident=f_uri, label=f'Knoedler Sales Book {book}, Folio {folio}')

				s_hmo.carries = s_text
				f_hmo.carries = f_text
				f_text.part_of = s_text
				f_hmo.part_of = s_hmo

				data['_physical_objects'].extend([add_crom_data({}, s_hmo), add_crom_data({}, f_hmo)])
				data['_linguistic_objects'].extend([add_crom_data({}, s_text), add_crom_data({}, f_text)])
				
				return f_text

			return vocab.BibliographyStatement(ident='', content=ref_text)
		return None

	def transaction_key_for_record(self, data, incoming=False):
		'''
		Return a URI representing the prov entry which the object (identified by the
		supplied data) is a part of. This may identify just the object being bought or
		sold or, in the case of multiple objects being bought for a single price, a
		single prov entry that encompasses multiple object acquisitions.
		'''
		rec = data['book_record']
		book_id = rec['stock_book_no']
		page_id = rec['page_number']
		row_id = rec['row_number']

		dir = 'In' if incoming else 'Out'
		
		price = data.get('purchase_knoedler_share') if incoming else data.get('sale_knoedler_share')
		if price:
			n = price.get('note')
			if n and n.startswith('for numbers '):
				return ('TX-MULTI', dir, n[12:])
		return ('TX', dir, book_id, page_id, row_id)

	@staticmethod
	def transaction_multiple_object_label(data, incoming=False):
		price = data.get('purchase_knoedler_share') if incoming else data.get('sale_knoedler_share')
		if price:
			n = price.get('note')
			if n and n.startswith('for numbers '):
				return n[12:]
		return None

	@staticmethod
	def transaction_contains_multiple_objects(data, incoming=False):
		'''
		Return `True` if the prov entry related to the supplied data represents a
		transaction of multiple objects with a single payment, `False` otherwise.
		'''
		price = data.get('purchase_knoedler_share') if incoming else data.get('sale_knoedler_share')
		if price:
			n = price.get('note')
			if n and n.startswith('for numbers '):
				return True
		return False

	def copy_source_information(self, dst: dict, src: dict):
		if not dst or not isinstance(dst, dict):
			return dst
		for k in self.csv_source_columns:
			with suppress(KeyError):
				dst[k] = src[k]
		return dst

	def make_object_uri(self, pi_rec_no, *uri_key):
		uri_key = list(uri_key)
		same_objects = self.services['same_objects_map']
		different_objects = self.services['different_objects']
		kn = uri_key[-1]
		if kn in different_objects:
			uri_key = uri_key[:-1] + ['flag-separated', kn, pi_rec_no]
		elif kn in same_objects:
			uri_key[-1] = same_objects[uri_key[-1]]
		uri = self.make_proj_uri(*uri_key)
		return uri

def delete_sellers(data, parent, services):
	sellers_to_be_deleted = services['sellers_to_be_deleted']
	if parent["pi_record_no"] in sellers_to_be_deleted:
		data = {k : [] for k in data}
	return data


def add_crom_price(data, parent, services, add_citations=False):
	'''
	Add modeling data for `MonetaryAmount` based on properties of the supplied `data` dict.
	'''
	helper = UtilityHelper('add_crom_price')
	currencies = services['currencies']
	amt = data.get('amount', '')
	if '[' in amt:
		data['amount'] = amt.replace('[', '').replace(']', '')
	amnt = extract_monetary_amount(data, currency_mapping=currencies, add_citations=add_citations, truncate_label_digits=2)
	if amnt and not amnt.id and data.get('amount'):
		amnt.id = helper.make_shared_uri('ATTR', 'MONEY', data.get('amount'))
	if amnt:
		add_crom_data(data=data, what=amnt)
	return data


def record_id(data):
	book = data['stock_book_no']
	page = data['page_number']
	row = data['row_number']
	return (book, page, row)

class AddPersonURI(Configurable):
	helper = Option(required=True)
	record_id = Option()

	def __call__(self, data:dict):
		pi = self.helper.person_identity
		pi.add_uri(data, record_id=self.record_id)
		# TODO: move this into MakeLinkedArtPerson
		auth_name = data.get('authority')
		if data.get('ulan'):
			ulan = data['ulan']
			data['uri'] = self.helper.make_shared_uri('PERSON', 'ULAN', ulan)
			data['ulan'] = ulan
		elif auth_name and '[' not in auth_name:
			data['uri'] = self.helper.make_shared_uri('PERSON', 'AUTHNAME', auth_name)
			data['identifiers'] = [
				vocab.PrimaryName(ident='', content=auth_name)
			]
		else:
			# not enough information to identify this person uniquely, so use the source location in the input file
			data['uri'] = self.helper.make_proj_uri('PERSON', 'PI_REC_NO', data['pi_record_no'])

		return data

class AddGroupURI(Configurable):
	helper = Option(required=True)

	def __call__(self, data:dict):
		# TODO: move this into MakeLinkedArtOrganization
		auth_name = data.get('authority')
		print(f'GROUP DATA: {pprint.pformat(data)}')
		if auth_name and '[' not in auth_name:
			data['uri'] = self.helper.make_shared_uri('GROUP', 'AUTHNAME', auth_name)
			data['identifiers'] = [
				vocab.PrimaryName(ident='', content=auth_name)
			]
		elif data.get('ulan'):
			ulan = data['ulan']
			data['uri'] = self.helper.make_shared_uri('GROUP', 'ULAN', ulan)
			data['ulan'] = ulan
		else:
			# not enough information to identify this person uniquely, so use the source location in the input file
			data['uri'] = self.helper.make_proj_uri('GROUP', 'PI_REC_NO', data['pi_record_no'])

		return data

class KnoedlerProvenance:
	def add_knoedler_creation_data(self, data):
		thing_label = data['label']
		knoedler = self.helper.static_instances.get_instance('Group', 'knoedler')
		ny = self.helper.static_instances.get_instance('Place', 'newyork')
		# TODO remove when development is completed
		# for key, value in self.helper.static_instances.instances['Place'].items():
		# 	t = self.helper.static_instances.get_instance('Place', key)
		
		o = get_crom_object(data)
		creation = model.Creation(ident='', label=f'Creation of {thing_label}')
		creation.carried_out_by = knoedler
		creation.took_place_at = ny
		o.created_by = creation

		return creation

class AddBook(Configurable, KnoedlerProvenance):
	helper = Option(required=True)
	make_la_lo = Service('make_la_lo')
	make_la_hmo = Service('make_la_hmo')
	static_instances = Option(default="static_instances")
	link_types = Service('link_types')

	def __call__(self, data:dict, make_la_lo, make_la_hmo, link_types):
		book = data['book_record']
		book_id, _, _ = record_id(book)

		book_type = model.Type(ident='http://vocab.getty.edu/aat/300028051', label='Book')
		book_type.classified_as = model.Type(ident='http://vocab.getty.edu/aat/300444970', label='Form')

		notes = []
		url = book.get('link')
		if url:
			link_data = link_types['link']
			label = link_data.get('label', url)
			description = link_data.get('field-description')
			if url.startswith('http'):
				page = vocab.DigitalImage(ident='', label=label)
				page._validate_range = False
				page.access_point = [vocab.DigitalObject(ident=url, label=url)]
				if description:
					page.referred_to_by = vocab.Note(ident='', content=description)
				notes.append(page)
			else:
				warnings.warn(f'*** Link value does not appear to be a valid URL: {url}')
				
		data['_text_book'] = {
			'uri': self.helper.make_proj_uri('Text', 'Book', book_id),
			'object_type': vocab.AccountBookText,
			'classified_as': [book_type],
			'label': f'Knoedler Stock Book {book_id}',
			'identifiers': [self.helper.knoedler_number_id(book_id, id_class=vocab.BookNumber)],
			'referred_to_by': notes
		}

		data['_physical_book'] = {
			'uri': self.helper.make_proj_uri('Book', book_id),
			'object_type': vocab.Book,
			'label': f'Knoedler Stock Book {book_id}',
			'identifiers': [self.helper.knoedler_number_id(book_id, id_class=vocab.BookNumber)],
			'carries': [data['_text_book']]
		}

		make_la_hmo(data['_physical_book'])
		make_la_lo(data['_text_book'])
		
		self.add_knoedler_creation_data(data['_text_book'])
		
		return data

class AddPage(Configurable, KnoedlerProvenance):
	helper = Option(required=True)
	make_la_lo = Service('make_la_lo')
	make_la_hmo = Service('make_la_hmo')
	static_instances = Option(default="static_instances")

	def __call__(self, data:dict, make_la_lo, make_la_hmo):
		book = data['book_record']
		book_id, page_id, _ = record_id(book)
		data['_text_page'] = {
			'uri': self.helper.make_proj_uri('Text', 'Book', book_id, 'Page', page_id),
			'object_type': [vocab.PageTextForm,vocab.AccountBookText],
			'label': f'Knoedler Stock Book {book_id}, Page {page_id}',
			'identifiers': [self.helper.knoedler_number_id(page_id, id_class=vocab.PageNumber)],
			'referred_to_by': [],
			'part_of': [data['_text_book']],
			'part': [],
		}

		if book.get('heading'):
			# This is a transcription of the heading of the page
			# Meaning it is part of the page linguistic object
			heading = book['heading']
			data['_text_page']['part'].append(add_crom_data(data={}, what=vocab.Heading(ident='', content=heading)))
			data['_text_page']['heading'] = heading # TODO: add heading handling to MakeLinkedArtLinguisticObject
		if book.get('subheading'):
			# Transcription of the subheading of the page
			subheading = book['subheading']
			data['_text_page']['part'].append(add_crom_data(data={}, what=vocab.Heading(ident='', content=subheading)))
			data['_text_page']['subheading'] = subheading # TODO: add subheading handling to MakeLinkedArtLinguisticObject

		make_la_lo(data['_text_page'])

		self.add_knoedler_creation_data(data['_text_page'])

		return data

class AddRow(Configurable, KnoedlerProvenance):
	helper = Option(required=True)
	make_la_lo = Service('make_la_lo')
	transaction_classification = Service('transaction_classification')
	static_instances = Option(default="static_instances")

	def __call__(self, data:dict, make_la_lo, transaction_classification):
		book = data['book_record']
		book_id, page_id, row_id = record_id(book)
		rec_num = data["star_record_no"]
		content = data['star_csv_data']
		
		row = vocab.Transcription(ident='', content=content)
		row.part_of = self.helper.static_instances.get_instance('LinguisticObject', 'db-knoedler')
		creation = vocab.TranscriptionProcess(ident='')
		creation.carried_out_by = self.helper.static_instances.get_instance('Group', 'gpi')
		row.created_by = creation
		row.identified_by = self.helper.gpi_number_id(rec_num, vocab.StarNumber)

		notes = []
		# TODO: add attributed star record number to row as a LocalNumber
		for k in ('description', 'working_note', 'verbatim_notes'):
			if book.get(k):
				notes.append(vocab.Note(ident='', content=book[k]))

		star_id = self.helper.gpi_number_id(rec_num, vocab.StarNumber)
		data['_text_row'] = {
			'uri': self.helper.make_proj_uri('Text', 'Book', book_id, 'Page', page_id, 'Row', row_id),
			'object_type': [vocab.EntryTextForm,vocab.AccountBookText],
			'label': f'Knoedler Stock Book {book_id}, Page {page_id}, Row {row_id}',
			'identifiers': [self.helper.knoedler_number_id(row_id, id_class=vocab.EntryNumber), star_id],
			'part_of': [data['_text_page']],
			'referred_to_by': notes,
			'also_found_on': row
		}
		make_la_lo(data['_text_row'])
		data['_record'] = data['_text_row']
		record = get_crom_object(data['_text_row'])
		transaction = data['book_record']['transaction']
		tx_cl = transaction_classification.get(transaction)
		if tx_cl:
			label = tx_cl.get('label')
			url = tx_cl.get('url')
			record.about = model.Type(ident=url, label=label)
		else:
			warnings.warn(f'*** No classification found for transaction type: {transaction!r}')

		creation = self.add_knoedler_creation_data(data['_text_row'])
		date = implode_date(data['entry_date'])
		if date:
			begin_date = implode_date(data['entry_date'], clamp='begin')
			end_date = implode_date(data['entry_date'], clamp='end')
			bounds = [begin_date, end_date]
			ts = timespan_from_outer_bounds(*bounds, inclusive=True)
			ts.identified_by = model.Name(ident='', content=date)
			creation.timespan = ts
		return data

class AddArtists(ProvenanceBase):
	helper = Option(required=True)
	make_la_person = Service('make_la_person')
	attribution_modifiers = Service('attribution_modifiers')
	attribution_group_types = Service('attribution_group_types')
	attribution_group_names = Service('attribution_group_names')
	
	def add_properties(self, data:dict, a:dict):
		sales_record = get_crom_object(data['_record'])
		a.setdefault('referred_to_by', [])
		a.update({
			'pi_record_no': data['pi_record_no'],
			'modifiers': self.modifiers(a),
		})

		if self.helper.person_identity.acceptable_person_auth_name(a.get('auth_name')):
			a.setdefault('label', a.get('auth_name'))
		a.setdefault('label', a.get('name'))

	def __call__(self, data:dict, *, make_la_person, attribution_modifiers, attribution_group_types, attribution_group_names):
		'''Add modeling for artists as people involved in the production of an object'''
		hmo = get_crom_object(data['_object'])
		
		self.model_artists_with_modifers(data, hmo, attribution_modifiers, attribution_group_types, attribution_group_names)
		return data

class PopulateKnoedlerObject(Configurable, pipeline.linkedart.PopulateObject):
	helper = Option(required=True)
	make_la_org = Service('make_la_org')
	vocab_type_map = Service('vocab_type_map')
	subject_genre = Service('subject_genre')
	subject_genre_style = Service('subject_genre_style')

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

	def _populate_object_visual_item(self, data:dict, object_data, title, subject_genre, subject_genre_style):
		sales_record = get_crom_object(data['_record'])
		hmo = get_crom_object(data)
		title = truncate_with_ellipsis(title, 100) or title

		# The visual item URI is just the object URI with a suffix. When URIs are
		# reconciled during prev/post sale rewriting, this will allow us to also reconcile
		# the URIs for the visual items (of which there should only be one per object)
		vi_uri = hmo.id + '-VisItem'
		vi = model.VisualItem(ident=vi_uri)
		vidata = {
			'uri': vi_uri,
			'referred_to_by': [sales_record],
		}
		if title:
			vidata['label'] = f'Visual work of “{title}”'
			sales_record = get_crom_object(data['_record'])
			vidata['names'] = [(title,{'referred_to_by': [sales_record]})]

		objgenre = object_data['genre'] if 'genre' in object_data else None
		objsubject = object_data['subject'] if 'subject' in object_data else None
		
		for prop, mapping in subject_genre.items():
			key = ', '.join((objsubject,objgenre)) if objsubject else objgenre
			try:
				aat_terms = mapping[key]
				for label, aat_url in aat_terms.items():
					t = model.Type(ident=aat_url, label=label)
					setattr(vi, prop, t)
			except:
				pass

		for prop, mapping in subject_genre_style.items():
			key = ', '.join((objsubject,objgenre)) if objsubject else objgenre
			try:
				aat_terms = mapping[key]
				for label, aat_url in aat_terms.items():
					t = model.Type(ident=aat_url, label=label)
					t.classified_as = model.Type(ident='http://vocab.getty.edu/aat/300015646', label='Styles and Periods (hierarchy name)')
					setattr(vi, prop, t)
			except:
				pass

		data['_visual_item'] = add_crom_data(data=vidata, what=vi)
		hmo.shows = vi

	def __call__(self, data:dict, *, vocab_type_map, make_la_org, subject_genre, subject_genre_style):
		sales_record = get_crom_object(data['_record'])
		data.setdefault('_physical_objects', [])
		data.setdefault('_linguistic_objects', [])
		data.setdefault('_people', [])

		odata = data['object']

		# split the title and reference in a value such as 「"Collecting her strength" title info from Sales Book 3, 1874-1879, f.252」
		label = self.helper.title_value(odata['title'])
		title_ref = self.helper.add_title_reference(data, odata['title'])

		typestring = odata.get('object_type', '')
		identifiers = []

# 		mlap = MakeLinkedArtPerson()
# 		for i, a in enumerate(data.get('_artists', [])):
# 			self.helper.copy_source_information(a, data)
# 			self.helper.person_identity.add_person(
# 				a,
# 				record=sales_record,
# 				relative_id=f'artist-{i}'
# 			)
# 			mlap(a)

		title_refs = [sales_record]
		if title_ref:
			title_refs.append(title_ref)
# 			warnings.warn(f'TODO: parse out citation information from title reference: {title_ref}')
		title = [label, {'referred_to_by': title_refs}]
		data['_object'] = {
			'title': title,
			'identifiers': identifiers,
			'referred_to_by': [sales_record],
			'_record': data['_record'],
			'_locations': [],
			'_organizations': [],
			'_text_row': data['_text_row'],
		}
		self.helper.copy_source_information(data['_object'], data)
		data['_object'].update({k:v for k,v in odata.items() if k in ('materials', 'dimensions', 'knoedler_number', 'present_location')})

		try:
			stock_number = odata['knoedler_number']
			uri_key	= ('Object', stock_number)
			identifiers.append(self.helper.knoedler_number_id(stock_number, vocab.StockNumber))
		except:
			uri_key = ('Object', 'Internal', data['pi_record_no'])
		uri = self.helper.make_object_uri(data['pi_record_no'], *uri_key)
		data['_object']['uri'] = uri
		data['_object']['uri_key'] = uri_key

		if typestring in vocab_type_map:
			clsname = vocab_type_map.get(typestring, None)
			otype = getattr(vocab, clsname)
			data['_object']['object_type'] = otype
		else:
			data['_object']['object_type'] = model.HumanMadeObject

		add_group_uri = AddGroupURI(helper=self.helper)
		consigner = self.helper.copy_source_information(data['consigner'], data)
# 		if consigner:
# 			add_group_uri(consigner)
# 			make_la_org(consigner)
# 			if 'no' in consigner:
# 				consigner_num = consigner['no']
# 				consigner_id = vocab.LocalNumber(ident='', label=f'Consigned number: {consigner_num}', content=consigner_num)
# 				data['_object']['identifiers'].append(consigner_id)
# 			data['_consigner'] = consigner

		mlao = MakeLinkedArtHumanMadeObject()
		mlao(data['_object'])

		self._populate_object_present_location(data['_object'])
		self._populate_object_visual_item(data['_object'], odata, label, subject_genre, subject_genre_style)
		self.populate_object_statements(data['_object'], default_unit='inches')
		data['_physical_objects'].append(data['_object'])
		return data

	def _populate_object_present_location(self, data:dict):
		sales_record = get_crom_object(data['_record'])
		hmo = get_crom_object(data)
		location = data.get('present_location')
		if location:
			loc = location.get('geog')
			note = location.get('note')
			tgn_data = location.get('loc_tgn')

			if loc:
				# TODO: if `parse_location_name` fails, still preserve the location string somehow
				current = parse_location_name(loc, uri_base=self.helper.uid_tag_prefix)
				inst = location.get('inst')
				if inst:
					owner_data = {
						'label': f'{inst} ({loc})',
						'identifiers': [
							model.Name(ident='', content=inst)
						]
					}
					ulan = None
					with suppress(ValueError, TypeError):
						ulan = int(location.get('insi'))
					if ulan:
						owner_data['ulan'] = ulan
						owner_data['uri'] = self.helper.make_proj_uri('ORG', 'ULAN', ulan)
					else:
						owner_data['uri'] = self.helper.make_proj_uri('ORG', 'NAME', inst, 'PLACE', loc)
				else:
					warnings.warn(f'*** Object present location data has a location, but not an institution: {pprint.pformat(data)}')
					owner_data = {
						'label': '(Anonymous organization)',
						'uri': self.helper.make_proj_uri('ORG', 'CURR-OWN', *now_key),
					}

				# It's conceivable that there could be more than one "present location"
				# for an object that is reconciled based on prev/post sale rewriting.
				# Therefore, the place URI must not share a prefix with the object URI,
				# otherwise all such places are liable to be merged during URI
				# reconciliation as part of the prev/post sale rewriting.
				
				owner_place = None
				if tgn_data:
					part_of = tgn_data.get("part_of") # this is a tgn id
					same_as = tgn_data.get('same_as') # this is a tgn id
					
					if part_of:
						tgn_instance = self.helper.static_instances.get_instance('Place', part_of)
						traverse_static_place_instances(self, tgn_instance)
						place = make_la_place(
							{
								'name': loc,
								'uri': self.helper.make_shared_uri(('PLACE',loc))
							},
						)
						o_place = get_crom_object(place)
						o_place.part_of = tgn_instance
						hmo.current_location = o_place
						owner_place = o_place
						data['_locations'].append(place)
					if same_as:
						tgn_instance = self.helper.static_instances.get_instance('Place', same_as)
						traverse_static_place_instances(self, tgn_instance)
						alternate_exists=False
						for id in tgn_instance.identified_by:
							if isinstance(id, vocab.AlternateName) and id.content == loc:
								alternate_exists = True

						if not alternate_exists:
							tgn_instance.identified_by = vocab.AlternateName(ident=self.helper.make_shared_uri(('PLACE',loc)), content=loc)
						
						hmo.current_location = tgn_instance
						owner_place = tgn_instance

				owner = None
				if owner_data:
					make_la_org = pipeline.linkedart.MakeLinkedArtOrganization()
					owner_data = make_la_org(owner_data)
					owner = get_crom_object(owner_data)
					hmo.current_owner = owner
					owner.residence = owner_place

				if note:
					owner_data['note'] = note
					desc = vocab.Description(ident='', content=note)
					if owner:
						assignment = model.AttributeAssignment(ident=self.helper.make_proj_uri('ATTR',  inst, loc))
						assignment.carried_out_by = owner
						desc.assigned_by = assignment
					hmo.referred_to_by = desc

				acc = location.get('acc')
				if acc:
					acc_number = vocab.AccessionNumber(ident='', content=acc)
					hmo.identified_by = acc_number
					assignment = model.AttributeAssignment(ident='')
					if owner:
						assignment = model.AttributeAssignment(ident=self.helper.make_shared_uri('ATTR','ACC',acc,'OWN', owner._label))
						assignment.carried_out_by = owner
					else:
						assignment = model.AttributeAssignment(ident=self.helper.make_shared_uri('ATTR','ACC',acc))
					acc_number.assigned_by = assignment

				
				data['_organizations'].append(owner_data)
				data['_final_org'] = owner_data
			else:
				pass # there is no present location place string

class TransactionSwitch:
	'''
	Wrap data values with an index on the transaction field so that different branches
	can be constructed for each transaction type (by using ExtractKeyedValue).
	'''
	def __call__(self, data:dict):
		rec = data['book_record']
		transaction = rec['transaction']
		if transaction in ('Sold', 'Destroyed', 'Stolen', 'Lost', 'Unsold', 'Returned'):
			yield {transaction: data}
		else:
			warnings.warn(f'TODO: handle transaction type {transaction}')

def prov_entry_label(helper, sale_type, transaction, rel, *key):
	if key[0] == 'TX-MULTI':
		_, dir, ids = key
		return f'Provenance Entry {rel} objects {ids}'
	else:
		_, dir, book_id, page_id, row_id = key
		return f'Provenance Entry {rel} object identified in book {book_id}, page {page_id}, row {row_id}'

class TransactionHandler(ProvenanceBase):
	def modifiers(self, a:dict, key:str):
		mod = a.get(key, '')
		mods = CaseFoldingSet({m.strip() for m in mod.split(';')} - {''})
		return mods

	def _empty_tx(self, data, incoming=False, purpose=None):
		tx_uri = self.helper.transaction_uri_for_record(data, incoming)
		tx_type = data.get('book_record', {}).get('transaction', 'Sold')
		if purpose == 'returning':
			tx = vocab.make_multitype_obj(vocab.SaleAsReturn, vocab.ProvenanceEntry, ident=tx_uri)
		else:
			tx = vocab.ProvenanceEntry(ident=tx_uri)
		
		sales_record = get_crom_object(data['_record'])
		tx.referred_to_by = sales_record
		
		return tx

	def _apprasing_assignment(self, data):
		rec = data['book_record']
		odata = data['_object']
		date = implode_date(data['entry_date'])
		
		hmo = get_crom_object(odata)
		sn_ident = self.helper.stock_number_identifier(odata, date)

		sellers = data['purchase_seller']
		price_info = data.get('purchase')
		if price_info and not len(sellers):
			# this inventorying has a "purchase" amount that is an evaluated worth amount
			amnt = get_crom_object(price_info)
			assignment = vocab.AppraisingAssignment(ident='', label=f'Evaluated worth of {sn_ident}')
			knoedler = self.helper.static_instances.get_instance('Group', 'knoedler')
			assignment.carried_out_by = knoedler
			assignment.assigned_property = 'dimension'
			assignment.assigned = amnt
			amnt.classified_as = model.Type(ident='http://vocab.getty.edu/aat/300412096', label='Valuation')
			assignment.assigned_to = hmo
			return assignment
		return None

	def _new_inventorying(self, data):
		rec = data['book_record']
		odata = data['_object']
		book_id, page_id, row_id = record_id(rec)
		date = implode_date(data['entry_date'])
		
		hmo = get_crom_object(odata)
		sn_ident = self.helper.stock_number_identifier(odata, date)
		inv_label = f'Knoedler Inventorying of {sn_ident}'

		inv_uri = self.helper.make_proj_uri('INV', book_id, page_id, row_id)
		inv = vocab.Inventorying(ident=inv_uri, label=inv_label)
		inv.identified_by = model.Name(ident='', content=inv_label)
		inv.encountered = hmo
		self.set_date(inv, data, 'entry_date')

		return inv

	def ownership_right(self, frac, person=None):
		if person:
			name = person._label
			right = vocab.OwnershipRight(ident='', label=f'{frac} ownership by {name}')
			right.possessed_by = person
		else:
			right = vocab.OwnershipRight(ident='', label=f'{frac} ownership')

		d = model.Dimension(ident='')
		d.unit = vocab.instances['percent']
		d.value = f'{float(100 * frac):.2f}'
		right.dimension = d
		return right

	def _add_prov_entry_rights(self, data:dict, tx, shared_people, incoming):
		knoedler = self.helper.static_instances.get_instance('Group', 'knoedler')
		sales_record = get_crom_object(data['_record'])

		hmo = get_crom_object(data['_object'])
		object_label = f'“{hmo._label}”'

		# this is the group of people along with Knoedler that made the purchase/sale (len > 1 when there is shared ownership)
		knoedler_group = [knoedler]
		if shared_people:
			people = []
			rights = []
			role = 'shared-buyer' if incoming else 'shared-seller'
			remaining = Fraction(1, 1)
# 			print(f'{1+len(shared_people)}-way split:')
			for i, p in enumerate(shared_people):
				person_dict = self.helper.copy_source_information(p, data)
				person = self.helper.add_person(
					person_dict,
					record=sales_record,
					relative_id=f'{role}_{i+1}'
				)
				name = p.get('name', p.get('auth_name', '(anonymous)'))
				share = p.get('share', '1/1')
				try:
					share_frac = Fraction(share)
					remaining -= share_frac

					right = self.ownership_right(share_frac, person)

					rights.append(right)
					people.append(person_dict)
					knoedler_group.append(person)
# 					print(f'   {share:<10} {name:<50}')
				except ValueError as e:
					warnings.warn(f'ValueError while handling shared rights ({e}): {pprint.pformat(p)}')
					raise
					
# 			print(f'   {str(remaining):<10} {knoedler._label:<50}')
			k_right = self.ownership_right(remaining, knoedler)
			rights.insert(0, k_right)

			total_right = vocab.OwnershipRight(ident='', label=f'Total Right of Ownership of {object_label}')
			total_right.applies_to = hmo
			for right in rights:
				total_right.part = right

			racq = model.RightAcquisition(ident='')
			racq.establishes = total_right
			tx.part = racq

			data['_people'].extend(people)

	def _add_prov_entry_payment(self, data:dict, tx, knoedler_price_part, price_info, people, people_agents, shared_people, shared_people_agents, date, incoming):
		knoedler = self.helper.static_instances.get_instance('Group', 'knoedler')
		knoedler_group = [knoedler]

		sales_record = get_crom_object(data['_record'])
		hmo = get_crom_object(data['_object'])
		sn_ident = self.helper.stock_number_identifier(data['_object'], date)
		
		price_data = {}
		if price_info and 'currency' in price_info:
			price_data['currency'] = price_info['currency']
		
		amnt = get_crom_object(price_info)
		knoedler_price_part_amnt = get_crom_object(knoedler_price_part)
		
		price_amount = None
		with suppress(AttributeError):
			price_amount = amnt.value
		parts = [(knoedler, knoedler_price_part_amnt)]
		if shared_people:
			role = 'shared-buyer' if incoming else 'shared-seller'
			for i, p in enumerate(shared_people):
				person_dict = self.helper.copy_source_information(p, data)
				person = self.helper.add_person(
					person_dict,
					record=sales_record,
					relative_id=f'{role}_{i+1}'
				)
				knoedler_group.append(person)
		# Check if a joint owner is either a seller or a buyer
		people_ids = set([x.id for x in people])
		knoedler_group_ids = set([x.id for x in knoedler_group])
		joint_owner_also_seller_or_buyer_id = people_ids.intersection(knoedler_group_ids)

		paym = None
		if amnt:
			tx_uri = tx.id
			payment_id = tx_uri + '-Payment'
			paym = model.Payment(ident=payment_id, label=f'Payment for {sn_ident}')
			tx.part = paym
			# If a joint owner is a seller or a buyer the payment node is empty and all the monetary ammount's information is moved to 
			# AttributeAssignment -> Monetary Ammount
			# P9->E13->p141->E97->P90->full_amount
			# P9->E13->p141->E97->P180->currency
			# P9->E13->p141->E97->p67i->E33->P190->note
			if not joint_owner_also_seller_or_buyer_id:
				paym.paid_amount = amnt
				for kp in knoedler_group:
					if incoming:
						paym.paid_from = kp
					else:
						paym.paid_to = kp
			else:
				assignment_id = tx_uri + '-Attribute assignment'
				assignment = model.AttributeAssignment(ident=assignment_id, label=f"Attribute assignment for {sn_ident}")
				assignment.assigned = amnt
				for kp in knoedler_group:
					assignment.carried_out_by = kp
				tx.part = assignment
			for p in shared_people_agents:
				# when an agent is acting on behalf of the buyer/seller, model their involvement in a sub-activity
				subpaym_role = 'Buyer' if incoming else 'Seller'
				subpaym = model.Activity(ident='', label=f"{subpaym_role}'s agent's role in payment")
				subpaym.classified_as = vocab.instances[f'{subpaym_role}sAgent']
				subpaym.carried_out_by = p
				paym.part = subpaym

			for i, partdata in enumerate(parts):
				person, part_amnt = partdata
				# add the part is there are multiple parts (shared tx), or if
				# this is the single part we know about, but its value is not
				#the same as the whole tx amount
				different_amount = False
				with suppress(AttributeError):
					if amnt.value != part_amnt.value:
						different_amount = True
				if len(parts) > 1 or different_amount:
					shared_payment_id = tx_uri + f'-Payment-{i}-share'
					shared_paym = model.Payment(ident=shared_payment_id, label=f"{person._label} share of payment for {sn_ident}")
					if part_amnt:
						shared_paym.paid_amount = part_amnt
					if incoming:
						shared_paym.paid_from = person
						# Partial payment of share from Knoedler to joint owner who is also the seller
						if joint_owner_also_seller_or_buyer_id:
							for purchase_buyer in people:
								shared_paym.paid_to = purchase_buyer
					else:
						shared_paym.paid_to = person
						# Partial payment of share to Knoedler from joint owner who is also the buyer
						if joint_owner_also_seller_or_buyer_id:
							for sale_buyer in people:
								shared_paym.paid_from = sale_buyer
					paym.part = shared_paym
					
		# If a joint owner is a seller or a buyer the payment node is empty
		if not joint_owner_also_seller_or_buyer_id:
			for person in people:
				if paym:
					if incoming:
						paym.paid_to = person
					else:
						paym.paid_from = person
		for p in people_agents:
			# when an agent is acting on behalf of the buyer/seller, model their involvement in a sub-activity
			if paym:
				subpaym_role = 'Seller' if incoming else 'Buyer'
				subpaym = model.Activity(ident='', label=f"{subpaym_role}'s agent's role in payment")
				subpaym.classified_as = vocab.instances[f'{subpaym_role}sAgent']
				subpaym.carried_out_by = p
				paym.part = subpaym

	def _add_prov_entry_acquisition(self, data:dict, tx, from_people, from_agents, to_people, to_agents, date, incoming, purpose=None):
		rec = data['book_record']
		book_id, page_id, row_id = record_id(rec)

		hmo = get_crom_object(data['_object'])
		sn_ident = self.helper.stock_number_identifier(data['_object'], date)

		dir = 'In' if incoming else 'Out'
		if purpose == 'returning':
			dir_label = 'Knoedler return'
		else:
			dir_label = 'Knoedler Purchase' if incoming else 'Knoedler Sale'
		acq_id = self.helper.make_proj_uri('ACQ', dir, book_id, page_id, row_id)
		acq = model.Acquisition(ident=acq_id)
		if self.helper.transaction_contains_multiple_objects(data, incoming):
			multi_label = self.helper.transaction_multiple_object_label(data, incoming)
			tx._label = f'{dir_label} of Stock Numbers {multi_label} ({date})'
			name = f'{dir_label} of {sn_ident}'
			acq._label = name
		else:
			sn_ident = self.helper.stock_number_identifier(data['_object'], date)
			name = f'{dir_label} of {sn_ident}'
			tx.identified_by = model.Name(ident='', content=name)
			tx._label = name
			acq._label = name
		acq.identified_by = model.Name(ident='', content=name)
		acq.transferred_title_of = hmo
		
		for p in from_people:
			acq.transferred_title_from = p
		for p in from_agents:
			# when an agent is acting on behalf of the seller, model their involvement in a sub-activity
			subacq = model.Activity(ident='', label="Seller's agent's role in acquisition")
			subacq.classified_as = vocab.instances['SellersAgent']
			subacq.carried_out_by = p
			acq.part = subacq
		for p in to_people:
			acq.transferred_title_to = p
		for p in to_agents:
			# when an agent is acting on behalf of the buyer, model their involvement in a sub-activity
			subacq = model.Activity(ident='', label="Buyer's agent's role in acquisition")
			subacq.classified_as = vocab.instances['BuyersAgent']
			subacq.carried_out_by = p
			acq.part = subacq

		tx.part = acq

	def _prov_entry(self, data, date_key, participants, price_info=None, knoedler_price_part=None, shared_people=None, incoming=False, purpose=None, buy_sell_modifiers=None):
		THROUGH = CaseFoldingSet(buy_sell_modifiers['through'])
		FOR = CaseFoldingSet(buy_sell_modifiers['for'])

		if shared_people is None:
			shared_people = []

		for k in ('_prov_entries', '_people', '_locations'):
			data.setdefault(k, [])

		parenthetical_parts = []
		date = implode_date(data[date_key]) if date_key in data else None
		if date:
			parenthetical_parts.append(date)
		
		odata = data['_object']
		sales_record = get_crom_object(data['_record'])

		tx = self._empty_tx(data, incoming, purpose=purpose)
		tx_uri = tx.id
		if 'knoedler_number' not in odata:
			tx.referred_to_by = vocab.Note(ident='', content='No Knoedler stock number was assigned to the object that is the subject of this provenance activity.')

		tx_data = add_crom_data(data={'uri': tx_uri}, what=tx)
		if date_key:
			self.set_date(tx, data, date_key)

		role = 'seller' if incoming else 'buyer'
		people_data = [
			self.helper.copy_source_information(p, data)
			for p in participants
		]
		
		people = []
		people_agents = []
		for i, p_data in enumerate(people_data):
			mod = self.modifiers(p_data, 'auth_mod')
			person = self.helper.add_person(
				p_data,
				record=sales_record,
				relative_id=f'{role}_{i+1}'
			)
			
			tgn_data = p_data.get('loc_tgn')
			if tgn_data:
				part_of = tgn_data.get("part_of") # this is a tgn id
				same_as = tgn_data.get('same_as') # this is a tgn id
				
				location_name = p_data.get('auth_loc', None) or p_data.get('auth_addr', None) or p_data.get('loc', None)
				
				if part_of:
					tgn_instance = self.helper.static_instances.get_instance('Place', part_of)
					traverse_static_place_instances(self, tgn_instance)					
					place = make_la_place(
						{
							'name': location_name,
							'uri': self.helper.make_shared_uri(('PLACE',location_name))
						},
					)
					o_place = get_crom_object(place)
					o_place.part_of = tgn_instance
					person.residence = o_place					
					
					data['_locations'].append(place)
				
				if same_as:
					tgn_instance = self.helper.static_instances.get_instance('Place', same_as)
					traverse_static_place_instances(self, tgn_instance)
					
					alternate_exists=False
					for id in tgn_instance.identified_by:
						if isinstance(id, vocab.AlternateName) and id.content == location_name:
							alternate_exists = True
					if not alternate_exists: 
						tgn_instance.identified_by = vocab.AlternateName(ident=self.helper.make_shared_uri(('PLACE',location_name)), content=location_name)
					
					person.residence = tgn_instance
				# else:
					# warning warn
					# pass
			else:
				# model in place with little info
				pass
			if THROUGH.intersects(mod):
				people_agents.append(person)
			else:
				people.append(person)
		
		knoedler = self.helper.static_instances.get_instance('Group', 'knoedler')
		knoedler_group = [knoedler]
		knoedler_group_agents = []
		if shared_people:
			# these are the people that joined Knoedler in the purchase/sale
			role = 'shared-buyer' if incoming else 'shared-seller'
			for i, p_data in enumerate(shared_people):
				mod = self.modifiers(p_data, 'auth_mod')
				person_dict = self.helper.copy_source_information(p_data, data)
				person = self.helper.add_person(
					person_dict,
					record=sales_record,
					relative_id=f'{role}_{i+1}'
				)
				if THROUGH.intersects(mod):
					knoedler_group_agents.append(person)
				else:
					knoedler_group.append(person)

		from_people = []
		from_agents = []
		to_people = []
		to_agents = []
		if incoming:
			from_people = people
			from_agents = people_agents
			to_people = knoedler_group
			to_agents = knoedler_group_agents
		else:
			from_people = knoedler_group
			from_agents = knoedler_group_agents
			to_people = people
			to_agents = people_agents

		if incoming:
			self._add_prov_entry_rights(data, tx, shared_people, incoming)
		self._add_prov_entry_payment(data, tx, knoedler_price_part, price_info, people, people_agents, shared_people, knoedler_group_agents, date, incoming)
		self._add_prov_entry_acquisition(data, tx, from_people, from_agents, to_people, to_agents, date, incoming, purpose=purpose)

# 		print('People:')
# 		for p in people:
# 			print(f'- {getattr(p, "_label", "(anonymous)")}')
# 		print('Shared People:')
# 		for p in shared_people:
# 			print(f'- {getattr(p, "_label", "(anonymous)")}')
# # 		self._add_prov_entry_custody_transfer(data, tx, people, incoming)

		data['_prov_entries'].append(tx_data)
		data['_people'].extend(people_data)
		return tx

	def add_return_tx(self, data, buy_sell_modifiers):
		rec = data['book_record']
		book_id, page_id, row_id = record_id(rec)

		purch_info = data.get('purchase')
		sale_info = data.get('sale')
		sellers = data['purchase_seller']
		shared_people = []
		for p in sellers:
			self.helper.copy_source_information(p, data)
		in_tx = self._prov_entry(data, 'entry_date', sellers, purch_info, incoming=True, buy_sell_modifiers=buy_sell_modifiers)
		out_tx = self._prov_entry(data, 'entry_date', sellers, sale_info, incoming=False, purpose='returning', buy_sell_modifiers=buy_sell_modifiers)
		return (in_tx, out_tx)

	def add_incoming_tx(self, data, buy_sell_modifiers):
		price_info = data.get('purchase')
		knoedler_price_part = data.get('purchase_knoedler_share')
		shared_people = data.get('purchase_buyer')
		sellers = data['purchase_seller']
		for p in sellers:
			self.helper.copy_source_information(p, data)
		
		tx = self._prov_entry(data, 'entry_date', sellers, price_info, knoedler_price_part, shared_people, incoming=True, buy_sell_modifiers=buy_sell_modifiers)
		
		prev_owners = data.get('prev_own', [])
		lot_object_key = self.helper.transaction_key_for_record(data, incoming=True)
		if prev_owners:
			self.model_prev_owners(data, prev_owners, tx, lot_object_key)

		return tx

	def model_prev_owners(self, data, prev_owners, tx, lot_object_key):
		sales_record = get_crom_object(data['_record'])
		for i, p in enumerate(prev_owners):
			role = 'prev_own'
			person_dict = self.helper.copy_source_information(p, data)
			person = self.helper.add_person(
				person_dict,
				record=sales_record,
				relative_id=f'{role}_{i+1}'
			)

			location_name = p.get('loc', None)
			if location_name:
				tgn_data = p.get('loc_tgn')
				if tgn_data:
					part_of = tgn_data.get("part_of") # this is a tgn id
					same_as = tgn_data.get('same_as') # this is a tgn id

					if part_of:
						tgn_instance = self.helper.static_instances.get_instance('Place', part_of)
						traverse_static_place_instances(self, tgn_instance)					
						place = make_la_place(
							{
								'name': location_name,
								'uri': self.helper.make_shared_uri(('PLACE',location_name))
							},
						)
						o_place = get_crom_object(place)
						o_place.part_of = tgn_instance
						person.residence = o_place					
						
						data['_locations'].append(place)
					
					if same_as:
						tgn_instance = self.helper.static_instances.get_instance('Place', same_as)
						traverse_static_place_instances(self, tgn_instance)
						
						alternate_exists=False
						for id in tgn_instance.identified_by:
							if isinstance(id, vocab.AlternateName) and id.content == location_name:
								alternate_exists = True
						if not alternate_exists:
							tgn_instance.identified_by = vocab.AlternateName(ident=self.helper.make_shared_uri(('PLACE',location_name)), content=location_name)
				
						person.residence = tgn_instance

			data['_people'].append(person_dict)

		ts = None # TODO
		prev_post_owner_records = [(prev_owners, True)]

		data['_record'] = data['_record']
		hmo = get_crom_object(data['_object'])
		for owner_data, rev in prev_post_owner_records:
			if rev:
				rev_name = 'prev-owner'
			else:
				rev_name = 'post-owner'
# 			ignore_fields = {'own_so', 'own_auth_l', 'own_auth_d'}
			tx_data = add_crom_data(data={}, what=tx)
			for seq_no, owner_record in enumerate(owner_data):
				record_id = f'{rev_name}-{seq_no+1}'
# 				if not any([bool(owner_record.get(k)) for k in owner_record.keys() if k not in ignore_fields]):
# 					# some records seem to have metadata (source information, location, or notes)
# 					# but no other fields set these should not constitute actual records of a prev/post owner.
# 					continue
				self.handle_prev_post_owner(data, hmo, tx_data, 'Sold', lot_object_key, owner_record, record_id, rev, ts, make_label=prov_entry_label)

	def add_outgoing_tx(self, data, buy_sell_modifiers):
		price_info = data.get('sale')
		knoedler_price_part = data.get('sale_knoedler_share')
		shared_people = data.get('purchase_buyer')
		buyers = data['sale_buyer']
		for p in buyers:
			self.helper.copy_source_information(p, data)
		return self._prov_entry(data, 'sale_date', buyers, price_info, knoedler_price_part, shared_people, incoming=False, buy_sell_modifiers=buy_sell_modifiers)

	@staticmethod
	def set_date(event, data, date_key, date_key_prefix=''):
		'''Associate a timespan with the event.'''
		date = implode_date(data[date_key], date_key_prefix)
		if date:
			begin = implode_date(data[date_key], date_key_prefix, clamp='begin')
			end = implode_date(data[date_key], date_key_prefix, clamp='eoe')
			bounds = [begin, end]
		else:
			bounds = []
		if bounds:
			ts = timespan_from_outer_bounds(*bounds)
			ts.identified_by = model.Name(ident='', content=date)
			event.timespan = ts

class ModelDestruction(TransactionHandler):
	helper = Option(required=True)
	make_la_person = Service('make_la_person')
	buy_sell_modifiers = Service('buy_sell_modifiers')
	transaction_classification = Service('transaction_classification')

	def __call__(self, data:dict, make_la_person, buy_sell_modifiers, transaction_classification):
		rec = data['book_record']
		date = implode_date(data['sale_date'])
		hmo = get_crom_object(data['_object'])

		title = self.helper.title_value(data['_object'].get('title'))
		short_title = truncate_with_ellipsis(title, 100) or title

		# The destruction URI is just the object URI with a suffix. When URIs are
		# reconciled during prev/post sale rewriting, this will allow us to also reconcile
		# the URIs for the destructions (of which there should only be one per object)
		dest_uri = hmo.id + '-Destruction'
		d = model.Destruction(ident=dest_uri, label=f'Destruction of “{short_title}”')
		if rec.get('verbatim_notes'):
			d.referred_to_by = vocab.Note(ident='', content=rec['verbatim_notes'])
		hmo.destroyed_by = d

		in_tx = self.add_incoming_tx(data, buy_sell_modifiers)
		in_tx_cl = transaction_classification.get('Purchase')
		in_tx.classified_as = model.Type(ident=in_tx_cl.get('url'), label=in_tx_cl.get('label'))

		return data

class ModelTheftOrLoss(TransactionHandler):
	helper = Option(required=True)
	make_la_person = Service('make_la_person')
	buy_sell_modifiers = Service('buy_sell_modifiers')
	transaction_classification = Service('transaction_classification')

	def __call__(self, data:dict, make_la_person, buy_sell_modifiers, transaction_classification):
		rec = data['book_record']
		pi_rec = data['pi_record_no']
		hmo = get_crom_object(data['_object'])
		sn_ident = self.helper.stock_number_identifier(data['_object'], None)
		
		in_tx = self.add_incoming_tx(data, buy_sell_modifiers)
		in_tx_cl = transaction_classification.get('Purchase')
		in_tx.classified_as = model.Type(ident=in_tx_cl.get('url'), label=in_tx_cl.get('label'))

		tx_out = self._empty_tx(data, incoming=False)

		tx_type = rec['transaction']
		label_type = None
		if tx_type == 'Lost':
			label_type = 'Loss'
			transfer_class = vocab.Loss
		else:
			label_type = 'Theft'
			transfer_class = vocab.Theft

		tx_cl = transaction_classification.get(tx_type)
		if tx_cl:
			label = tx_cl.get('label')	
			url = tx_cl.get('url')
			tx_out.classified_as = model.Type(ident=url,label=label)
		else:
			warnings.warn(f'*** No classification found for transaction type: {tx_type!r}')

		tx_out._label = f'{label_type} of {sn_ident}'
		tx_out.identified_by = model.Name(ident='', content=tx_out._label)
		tx_out_data = add_crom_data(data={'uri': tx_out.id, 'label': tx_out._label}, what=tx_out)

		title = self.helper.title_value(data['_object'].get('title'))
		short_title = truncate_with_ellipsis(title, 100) or title

		# It's conceivable that there could be more than one theft of an object (if it was
		# recovered after the first theft). Therefore, the theft URI must not share a
		# prefix with the object URI, otherwise all such thefts would be merged during
		# URI reconciliation as part of the prev/post sale rewriting.
		theft_uri = hmo.id.replace('#', f'#{label_type.upper()},')

		warnings.warn('TODO: parse Theft/Loss note for date and location')
		# Examples:
		#     "Dec 1947 Looted by Germans during war"
		#     "July 1959 Lost in Paris f.111"
		#     "Lost at Sea on board Str Europe lost April 4/74"

		notes = rec.get('verbatim_notes')
		if notes and 'Looted' in notes:
			transfer_class = vocab.Looting
		t = transfer_class(ident=theft_uri, label=f'{label_type} of “{short_title}”')
		t.transferred_custody_from = self.helper.static_instances.get_instance('Group', 'knoedler')
		t.transferred_custody_of = hmo

		if notes:
			t.referred_to_by = vocab.Note(ident='', content=notes)

		tx_out.part = t

		data['_prov_entries'].append(tx_out_data)
		return data

class ModelFinalSale(TransactionHandler):
	'''
	Add ProvenanceEntry/Acquisition modeling for the sale leading to the final
	known location (owner) of an object.
	'''
	helper = Option(required=True)
	make_la_person = Service('make_la_person')
	buy_sell_modifiers = Service('buy_sell_modifiers')

	def __call__(self, data:dict, make_la_person, buy_sell_modifiers):
		data = data.copy()
		
		# reset prov entries and people because we're only interested in those
		# related to the final sale on this branch of the graph
		odata = data['_object'].copy()
		sales_record = get_crom_object(data['_record'])
		self.helper.copy_source_information(data, odata)

		odata.setdefault('_prov_entries', [])
		odata.setdefault('_people', [])
		hmo = get_crom_object(odata)
		org = odata.get('_final_org')
		if org:
			odata['_record'] = data['_record']
			rec = data['book_record']
			book_id = rec['stock_book_no']
			page_id = rec['page_number']
			row_id = rec['row_number']

			price_info = None
			knoedler_price_part = None
			shared_people = [org]
			sellers = []
			date_key = None
			tx = self._prov_entry(data, date_key, sellers, price_info, knoedler_price_part, shared_people, incoming=True, buy_sell_modifiers=buy_sell_modifiers)
		
			current_tx = self.add_outgoing_tx(data, buy_sell_modifiers)
			current_tx_data = add_crom_data(data={}, what=current_tx)
		
			lot_object_key = list(self.helper.transaction_key_for_record(data, incoming=True))
			tx_data = {'uri': tx.id, 'label': f'Event leading to the currently known location of {hmo._label}'}
			add_crom_data(data=tx_data, what=tx)
			
			self.handle_prev_post_owner(odata, hmo, current_tx_data, 'Sold', lot_object_key, org, f'final-owner-1', False, None, make_label=prov_entry_label)
			odata = {k: v for k, v in odata.items() if k in ('_prov_entries', '_people')}
			yield odata

class ModelSale(TransactionHandler):
	'''
	Add ProvenanceEntry/Acquisition modeling for a sold object. This includes an acquisition
	TO Knoedler from seller(s), and another acquisition FROM Knoedler to buyer(s).
	'''
	helper = Option(required=True)
	make_la_person = Service('make_la_person')
	buy_sell_modifiers = Service('buy_sell_modifiers')
	transaction_classification = Service('transaction_classification')

	def __call__(self, data:dict, make_la_person, buy_sell_modifiers, transaction_classification, in_tx=None, out_tx=None):
		sellers = data['purchase_seller']
		if not in_tx:
			if len(sellers):
				# if there are sellers in this record, then model the incoming transaction.
				in_tx = self.add_incoming_tx(data, buy_sell_modifiers)
				tx_cl = transaction_classification.get('Purchase')
				in_tx.classified_as = model.Type(ident=tx_cl.get('url'), label=tx_cl.get('label'))
			else:
				# if there are no sellers, then this is an object that was previously unsold, and should be modeled as an inventory activity
				inv = self._new_inventorying(data)
				appraisal = self._apprasing_assignment(data)
				inv_label = inv._label
				in_tx = self._empty_tx(data, incoming=True)
				in_tx.part = inv
				if appraisal:
					in_tx.part = appraisal
				in_tx.identified_by = model.Name(ident='', content=inv_label)
				in_tx._label = inv_label
				in_tx_data = add_crom_data(data={'uri': in_tx.id, 'label': inv_label}, what=in_tx)
				data.setdefault('_prov_entries', [])
				data['_prov_entries'].append(in_tx_data)

		if not out_tx:
			out_tx = self.add_outgoing_tx(data, buy_sell_modifiers)

			transaction = data['book_record']['transaction']
			tx_cl = transaction_classification.get(transaction)
			if tx_cl:
				label = tx_cl.get('label')	
				url = tx_cl.get('url')
				out_tx.classified_as = model.Type(ident=url,label=label)
			else:
				warnings.warn(f'*** No classification found for transaction type: {transaction!r}')

		in_tx.ends_before_the_start_of = out_tx
		out_tx.starts_after_the_end_of = in_tx
		yield data

class ModelReturn(ModelSale):
	helper = Option(required=True)
	make_la_person = Service('make_la_person')
	buy_sell_modifiers = Service('buy_sell_modifiers')
	transaction_classification = Service('transaction_classification')

	def __call__(self, data:dict, make_la_person, buy_sell_modifiers, transaction_classification):
		sellers = data.get('purchase_seller', [])
		buyers = data.get('sale_buyer', [])
		if not buyers:
			buyers = sellers.copy()
			data['sale_buyer'] = buyers
		in_tx, out_tx = self.add_return_tx(data, buy_sell_modifiers)
		in_tx_cl = transaction_classification.get('Purchase')
		in_tx.classified_as = model.Type(ident=in_tx_cl.get('url'), label=in_tx_cl.get('label'))
		yield from super().__call__(data, make_la_person, buy_sell_modifiers, transaction_classification,in_tx=in_tx, out_tx=out_tx)

class ModelUnsoldPurchases(TransactionHandler):
	helper = Option(required=True)
	make_la_person = Service('make_la_person')
	buy_sell_modifiers = Service('buy_sell_modifiers')
	transaction_classification = Service('transaction_classification')

	def __call__(self, data:dict, make_la_person, buy_sell_modifiers, transaction_classification):
		rec = data['book_record']
		pi_rec = data['pi_record_no']
		odata = data['_object']
		book_id, page_id, row_id = record_id(rec)
		sales_record = get_crom_object(data['_record'])
		date = implode_date(data['entry_date'])
		
		sellers = data['purchase_seller']
		if len(sellers) == 0:
			# if there are no sellers in this record (and it is "Unsold" by design of the caller),
			# then this is actually an Inventorying event, and handled in ModelInventorying
			return

		hmo = get_crom_object(odata)
		object_label = f'“{hmo._label}”'

		sn_ident = self.helper.stock_number_identifier(odata, date)

		in_tx = self.add_incoming_tx(data, buy_sell_modifiers)
		in_tx_cl = transaction_classification.get('Purchase')
		in_tx.classified_as = model.Type(ident=in_tx_cl.get('url'), label=in_tx_cl.get('label'))
		yield data

class ModelInventorying(TransactionHandler):
	helper = Option(required=True)
	make_la_person = Service('make_la_person')
	buy_sell_modifiers = Service('buy_sell_modifiers')
	transaction_classification = Service('transaction_classification')

	def __call__(self, data:dict, make_la_person, buy_sell_modifiers, transaction_classification):
		rec = data['book_record']
		pi_rec = data['pi_record_no']
		odata = data['_object']
		book_id, page_id, row_id = record_id(rec)
		sales_record = get_crom_object(data['_record'])
		date = implode_date(data['entry_date'])
		for k in ('_prov_entries', '_people'):
			data.setdefault(k, [])
		
		sellers = data['purchase_seller']
		if len(sellers) > 0:
			# if there are sellers in this record (and it is "Unsold" by design of the caller),
			# then this is not an actual Inventorying event, and handled in ModelUnsoldPurchases
			return

		hmo = get_crom_object(odata)
		object_label = f'“{hmo._label}”'

		sn_ident = self.helper.stock_number_identifier(odata, date)

		inv = self._new_inventorying(data)
		appraisal = self._apprasing_assignment(data)
		inv_label = inv._label

		tx_out = self._empty_tx(data, incoming=False)
		tx_out._label = inv_label
		tx_out.identified_by = model.Name(ident='', content=inv_label)

		transaction = rec['transaction']
		tx_cl = transaction_classification.get(transaction)
		if tx_cl:
			label = tx_cl.get('label')
			url = tx_cl.get('url')
			tx_out.classified_as = model.Type(ident=url,label=label)
		else:
			warnings.warn(f'*** No classification found for transaction type: {transaction!r}')

		inv_uri = self.helper.make_proj_uri('INV', book_id, page_id, row_id)
		inv = vocab.Inventorying(ident=inv_uri, label=inv_label)
		inv.identified_by = model.Name(ident='', content=inv_label)
		inv.encountered = hmo
		inv.carried_out_by = self.helper.static_instances.get_instance('Group', 'knoedler')
		self.set_date(inv, data, 'entry_date')

		tx_out.part = inv
		if appraisal:
			tx_out.part = appraisal
		self.set_date(tx_out, data, 'entry_date')

		tx_out_data = add_crom_data(data={'uri': tx_out.id, 'label': inv_label}, what=tx_out)
		data['_prov_entries'].append(tx_out_data)

		yield data

#mark - Knoedler Pipeline class

class KnoedlerPipeline(PipelineBase):
	'''Bonobo-based pipeline for transforming Knoedler data from CSV into JSON-LD.'''
	def __init__(self, input_path, data, **kwargs):
		project_name = 'knoedler'
		self.input_path = input_path
		self.services = None

		helper = KnoedlerUtilityHelper(project_name)
		super().__init__(project_name, helper=helper)
		helper.static_instances = self.static_instances

		vocab.register_instance('form type', {'parent': model.Type, 'id': '300444970', 'label': 'Form'})

		vocab.register_vocab_class('ConstructedTitle', {'parent': model.Name, 'id': '300417205', 'label': 'Constructed Title'})

		vocab.register_vocab_class('SaleAsReturn', {"parent": model.Activity, "id":"300445014", "label": "Sale (Return to Original Owner)"})

		vocab.register_vocab_class('EntryNumber', {"parent": model.Identifier, "id":"300445023", "label": "Entry Number"})
		vocab.register_vocab_class('PageNumber', {"parent": model.Identifier, "id":"300445022", "label": "Page Number"})
		vocab.register_vocab_class('BookNumber', {"parent": model.Identifier, "id":"300445021", "label": "Book Number"})

		vocab.register_vocab_class('PageTextForm', {"parent": model.LinguisticObject, "id":"300194222", "label": "Page", "metatype": "form type"})
		vocab.register_vocab_class('EntryTextForm', {"parent": model.LinguisticObject, "id":"300438434", "label": "Entry", "metatype": "form type"})

		self.graph = None
		self.models = kwargs.get('models', settings.arches_models)
		self.header_file = data['header_file']
		self.files_pattern = data['files_pattern']
		self.limit = kwargs.get('limit')
		self.debug = kwargs.get('debug', False)

		fs = bonobo.open_fs(input_path)
		with fs.open(self.header_file, newline='') as csvfile:
			r = csv.reader(csvfile)
			self.headers = [v.lower() for v in next(r)]

	def _construct_same_object_map(self, same_objects):
		'''
		Same objects data comes in as a list of identity equivalences (each being a list of ID strings).
		ID strings may appear in multiple equivalences. For example, these 3 equivalences
		represent a single object with 4 ID strings:

			[['1','2','3'], ['1','3'], ['2','4']]

		This function computes a dict mapping every ID string to a canonical
		representative ID for that object (being the first ID value, lexicographically):

			{
				'1': '1',
				'2': '1',
				'3': '1',
				'4': '1',
			}
		'''
		same_objects_map = {}
		same_objects_map = {k: sorted(l) for l in same_objects for k in l}
		for k in same_objects_map:
			v = same_objects_map[k]
			orig = set(v)
			vv = set(v)
			for kk in v:
				vv |= set(same_objects_map[kk])
			if vv != orig:
				keys = v + [k]
				for kk in keys:
					same_objects_map[kk] = sorted(vv)

		same_object_id_map = {k: v[0] for k, v in same_objects_map.items()}
		leaders = set()
		for k in same_objects_map:
			leaders.add(same_objects_map[k][0])
		return same_object_id_map

	def setup_services(self):
		'''Return a `dict` of named services available to the bonobo pipeline.'''
		services = super().setup_services()

		people_groups = set()
		pg_file = pathlib.Path(settings.pipeline_tmp_path).joinpath('people_groups.json')
		with suppress(FileNotFoundError):
			with pg_file.open('r') as fh:
				data = json.load(fh)
				for key in data['group_keys']:
					people_groups.add(tuple(key))
		services['people_groups'] = people_groups

		same_objects = services.get('objects_same', {}).get('objects', [])
		same_object_id_map = self._construct_same_object_map(same_objects)
		services['same_objects_map'] = same_object_id_map

		different_objects = services.get('objects_different', {}).get('knoedler_numbers', [])
		services['different_objects'] = different_objects

		sellers_to_be_deleted = services.get('sellers_to_be_deleted', {}).get('pi_record_no', [])
		services['sellers_to_be_deleted'] = sellers_to_be_deleted
		
		# lookup dictionary with all authority information retrieved
		tgn_places = services.get('tgn', {}) 
		
		# lookup dictionary that maps knoedler a field and its value to a place 
		# either as same as or as falling within a place in tgn_places dict
		knoedler_tgn = services.get('knoedler_tgn', {}) 
		
		services['tgn'] = tgn_places
		services['knoedler_tgn'] = knoedler_tgn
		
		# make these case-insensitive by wrapping the value lists in CaseFoldingSet
		for name in ('attribution_modifiers',):
			if name in services:
				services[name] = {k: CaseFoldingSet(v) for k, v in services[name].items()}

		if 'attribution_modifiers' in services:
			attribution_modifiers = services['attribution_modifiers']
			PROBABLY = attribution_modifiers['probably by']
			POSSIBLY = attribution_modifiers['possibly by']
			attribution_modifiers['uncertain'] = PROBABLY | POSSIBLY

		services.update({
			# to avoid constructing new MakeLinkedArtPerson objects millions of times, this
			# is passed around as a service to the functions and classes that require it.
			'make_la_person': MakeLinkedArtPerson(),
			'make_la_lo': MakeLinkedArtLinguisticObject(),
			'make_la_hmo': MakeLinkedArtHumanMadeObject(),
			'make_la_org': MakeLinkedArtOrganization(),
			'counts': defaultdict(int)
		})
		return services
	
	def _static_place_instances(self):
		'''
		Create static instances for every place mentioned in the tgn service data.
		'''
		super()._static_place_instances()
		tgn_places = self.services['tgn']
		instances = {}
		places = {}
		
		for tgn_id, tgn_data in tgn_places.items():
			places[tgn_id] = tgn_data
		
		start = timeit.default_timer()	
		print("Started the tranformation of Static Places Instances...")
		for tgn_id, tgn_data in places.items():
			tgn_id = tgn_data.get('tgn_id')		
					
			place = make_tgn_place(tgn_data, self.helper.make_shared_uri, tgn_places)
			instances[tgn_id] = place
		# import pdb; pdb.set_trace()
		print(f"Completed in {timeit.default_timer() - start}")
		return instances

	def add_sales_chain(self, graph, records, services, serialize=True):
		'''Add transformation of sales records to the bonobo pipeline.'''
		sales_records = graph.add_chain(
# 			"star_record_no",
# 			"pi_record_no",
			PreserveCSVFields(key='star_csv_data', order=self.headers),
			KeyManagement(
				drop_empty=True,
				operations=[
					{
						'group': {
							'present_location': {
								'postprocess': [
									lambda x, _: strip_key_prefix('present_loc_', x),
									lambda d, p: associate_with_tgn_record(d, p, services['knoedler_tgn'],"present_loc_geog"),
								],
								'properties': (
									"present_loc_geog",
									"present_loc_inst",
									"present_loc_acc",
									"present_loc_note",
								)
							},
						}
					},
					{
						'group_repeating': {
							'_artists': {
								'rename_keys': {
									"artist_name": 'name',
									"artist_authority": 'auth_name',
									"artist_nationality": 'nationality',
									"artist_attribution_mod": 'attribution_mod',
									"artist_attribution_mod_auth": 'attrib_mod_auth',
								},
								'postprocess': [
									filter_empty_person,
								],
								'prefixes': (
									"artist_name",
									"artist_authority",
									"artist_nationality",
									"artist_attribution_mod",
									"artist_attribution_mod_auth",
								)
							},
							'purchase_seller': {
								'postprocess': [
									lambda d, p: delete_sellers(d, p, services),
									filter_empty_person,
									lambda x, _: strip_key_prefix('purchase_seller_', x),
									lambda d, p: associate_with_tgn_record(d, p, services['knoedler_tgn'],"seller_loc"),
								],
								'prefixes': (
									"purchase_seller_name",
									"purchase_seller_loc",
									"purchase_seller_auth_name",
									"purchase_seller_auth_loc",
									"purchase_seller_auth_mod",
								)
							},
							'purchase_buyer': {
								'rename_keys': {
									'purchase_buyer_own': 'name',
									'purchase_buyer_share': 'share',
									'purchase_buyer_share_auth': 'auth_name',
								},
								'postprocess': [
									filter_empty_person,
								],
								'prefixes': (
									"purchase_buyer_own",
									"purchase_buyer_share",
									"purchase_buyer_share_auth",
								)
							},
							'prev_own': {
								'postprocess': [
									lambda x, _: strip_key_prefix('prev_own_', x),
									lambda d, p: associate_with_tgn_record(d, p, services['knoedler_tgn'],"prev_own_loc")
								],
								'rename_keys': {
									'prev_own': 'name',
									'prev_own_auth': 'auth_name',
									'prev_own_loc': 'loc',
								},
								'prefixes': (
									"prev_own",
									"prev_own_auth",
									"prev_own_loc",
								)
							},
							'sale_buyer': {
								'postprocess': [
									lambda x, _: strip_key_prefix('sale_buyer_', x),
									lambda d, p: associate_with_tgn_record(d, p, services['knoedler_tgn'],"buyer"),
								],
								'prefixes': (
									"sale_buyer_name",
									"sale_buyer_loc",
									"sale_buyer_auth_name",
									"sale_buyer_auth_addr",
									"sale_buyer_auth_mod",
								)
							}
						},
						'group': {
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
									"present_location",
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
								'rename_keys': {
									"purch_amount": 'amount',
									"purch_currency": 'currency',
									"purch_note": 'note',
								},
								'postprocess': [lambda d, p: add_crom_price(d, p, services)],
								'properties': (
									"purch_amount",
									"purch_currency",
									"purch_note",
								)
							},
							'sale': {
								'rename_keys': {
									"price_amount": 'amount',
									"price_currency": 'currency',
									"price_note": 'note',
								},
								'postprocess': [lambda d, p: add_crom_price(d, p, services)],
								'properties': (
									"price_amount",
									"price_currency",
									"price_note",
								)
							},
							'purchase_knoedler_share': {
								'rename_keys': {
									"knoedpurch_amt": 'amount',
									"knoedpurch_curr": 'currency',
									"knoedpurch_note": 'note',
								},
								'postprocess': [lambda d, p: add_crom_price(d, p, services)],
								'properties': (
									"knoedpurch_amt",
									"knoedpurch_curr",
									"knoedpurch_note",
								)
							},
							'sale_knoedler_share': {
								'rename_keys': {
									"knoedshare_amt": 'amount',
									"knoedshare_curr": 'currency',
									"knoedshare_note": 'note',
								},
								'postprocess': [lambda d, p: add_crom_price(d, p, services)],
								'properties': (
									"knoedshare_amt",
									"knoedshare_curr",
									"knoedshare_note",
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
								'rename_keys': {
									"post_owner": 'name',
									"post_owner_auth": 'auth_name',
								},
								'properties': (
									"post_owner",
									"post_owner_auth",
								)
							}
						}
					}
				]
			),
			RecordCounter(name='records', verbose=self.debug),
			_input=records.output
		)

		books = self.add_book_chain(graph, sales_records)
		pages = self.add_page_chain(graph, books)
		rows = self.add_row_chain(graph, pages)
		objects = self.add_object_chain(graph, rows)

		tx = graph.add_chain(
			TransactionSwitch(),
			_input=objects.output
		)
		return tx

	def add_transaction_chains(self, graph, tx, services, serialize=True):
		inventorying = graph.add_chain(
			ExtractKeyedValue(key='Unsold'),
			ModelInventorying(helper=self.helper),
			_input=tx.output
		)

		unsold_purchases = graph.add_chain(
			ExtractKeyedValue(key='Unsold'),
			ModelUnsoldPurchases(helper=self.helper),
			_input=tx.output
		)

		sale = graph.add_chain(
			ExtractKeyedValue(key='Sold'),
			ModelSale(helper=self.helper),
			_input=tx.output
		)

		returned = graph.add_chain(
			ExtractKeyedValue(key='Returned'),
			ModelReturn(helper=self.helper),
			_input=tx.output
		)

		destruction = graph.add_chain(
			ExtractKeyedValue(key='Destroyed'),
			ModelDestruction(helper=self.helper),
			_input=tx.output
		)

		theft = graph.add_chain(
			ExtractKeyedValue(key='Stolen'),
			ModelTheftOrLoss(helper=self.helper),
			_input=tx.output
		)

		loss = graph.add_chain(
			ExtractKeyedValue(key='Lost'),
			ModelTheftOrLoss(helper=self.helper),
			_input=tx.output
		)

		# activities are specific to the inventorying chain
		activities = graph.add_chain( ExtractKeyedValues(key='_activities'), _input=inventorying.output )
		if serialize:
			self.add_serialization_chain(graph, activities.output, model=self.models['Inventorying'])

		# people and prov entries can come from any of these chains:
		for branch in (sale, destruction, theft, loss, inventorying, unsold_purchases, returned):
			prov_entry = graph.add_chain( ExtractKeyedValues(key='_prov_entries'), _input=branch.output )
			people = graph.add_chain( ExtractKeyedValues(key='_people'), _input=branch.output )
			locations = graph.add_chain( ExtractKeyedValues(key='_locations'), _input=branch.output )

			if serialize:
				self.add_serialization_chain(graph, prov_entry.output, model=self.models['ProvenanceEntry'])
				self.add_serialization_chain(graph, locations.output, model=self.models['Place'])
				self.add_person_or_group_chain(graph, people)

	def add_book_chain(self, graph, sales_records, serialize=True):
		books = graph.add_chain(
# 			add_book,
			AddBook(static_instances=self.static_instances, helper=self.helper),
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
		act = graph.add_chain( ExtractKeyedValues(key='_activities'), _input=text.output )
		if serialize:
			self.add_serialization_chain(graph, act.output, model=self.models['ProvenanceEntry'])
			self.add_serialization_chain(graph, phys.output, model=self.models['HumanMadeObject'])
			self.add_serialization_chain(graph, text.output, model=self.models['LinguisticObject'])
		return books

	def add_page_chain(self, graph, books, serialize=True):
		pages = graph.add_chain(
			AddPage(static_instances=self.static_instances, helper=self.helper),
			_input=books.output
		)
		text = graph.add_chain(
			ExtractKeyedValue(key='_text_page'),
			_input=pages.output
		)
		act = graph.add_chain( ExtractKeyedValues(key='_activities'), _input=text.output )
		if serialize:
			self.add_serialization_chain(graph, act.output, model=self.models['ProvenanceEntry'])
			self.add_serialization_chain(graph, text.output, model=self.models['LinguisticObject'])
		return pages

	def add_row_chain(self, graph, pages, serialize=True):
		rows = graph.add_chain(
			AddRow(static_instances=self.static_instances, helper=self.helper),
			_input=pages.output
		)
		text = graph.add_chain(
			ExtractKeyedValue(key='_text_row'),
			_input=rows.output
		)
		act = graph.add_chain( ExtractKeyedValues(key='_activities'), _input=text.output )
		if serialize:
			self.add_serialization_chain(graph, act.output, model=self.models['ProvenanceEntry'])
			self.add_serialization_chain(graph, text.output, model=self.models['LinguisticObject'])
		return rows

	def add_object_chain(self, graph, rows, serialize=True):
		objects = graph.add_chain(
			PopulateKnoedlerObject(helper=self.helper),
			AddArtists(helper=self.helper),
			_input=rows.output
		)

		people = graph.add_chain( ExtractKeyedValues(key='_people'), _input=objects.output )
		hmos1 = graph.add_chain( ExtractKeyedValues(key='_physical_objects'), _input=objects.output )
		hmos2 = graph.add_chain( ExtractKeyedValues(key='_original_objects'), _input=objects.output )
		texts = graph.add_chain( ExtractKeyedValues(key='_linguistic_objects'), _input=objects.output )
		groups1 = graph.add_chain( ExtractKeyedValues(key='_organizations'), _input=objects.output )
		groups2 = graph.add_chain( ExtractKeyedValues(key='_organizations'), _input=hmos1.output )
		odata = graph.add_chain(
			ExtractKeyedValue(key='_object'),
			_input=objects.output
		)
		final_sale = graph.add_chain(
			ModelFinalSale(helper=self.helper),
			_input=objects.output
		)
		prov_entry = graph.add_chain(
			ExtractKeyedValues(key='_prov_entries'),
			_input=final_sale.output
		)
		people2 = graph.add_chain( ExtractKeyedValues(key='_people'), _input=final_sale.output )
		owners = self.add_person_or_group_chain(graph, hmos1, key='_other_owners', serialize=serialize)

		items = graph.add_chain(
			ExtractKeyedValue(key='_visual_item'),
			pipeline.linkedart.MakeLinkedArtRecord(),
			_input=hmos1.output
		)

		items2 = graph.add_chain(
			ExtractKeyedValue(key='_visual_item'),
			pipeline.linkedart.MakeLinkedArtRecord(),
			_input=hmos2.output
		)

# 		consigners = graph.add_chain( ExtractKeyedValue(key='_consigner'), _input=objects.output )
		artists = graph.add_chain(
			ExtractKeyedValues(key='_artists'),
			_input=objects.output
		)
		
		if serialize:
			self.add_serialization_chain(graph, items.output, model=self.models['VisualItem'])
			self.add_serialization_chain(graph, items2.output, model=self.models['VisualItem'])
			self.add_serialization_chain(graph, hmos1.output, model=self.models['HumanMadeObject'])
			self.add_serialization_chain(graph, hmos2.output, model=self.models['HumanMadeObject'])
			self.add_serialization_chain(graph, texts.output, model=self.models['LinguisticObject'])
# 			self.add_serialization_chain(graph, consigners.output, model=self.models['Group'])
			self.add_person_or_group_chain(graph, groups1)
			self.add_person_or_group_chain(graph, groups2)
			self.add_person_or_group_chain(graph, artists)
			self.add_person_or_group_chain(graph, people)
			self.add_person_or_group_chain(graph, people2)
			self.add_person_or_group_chain(graph, owners)
			self.add_person_or_group_chain(graph, odata, key='_organizations')
			self.add_serialization_chain(graph, prov_entry.output, model=self.models['ProvenanceEntry'])
			_ = self.add_places_chain(graph, odata, key='_locations', serialize=serialize, include_self=True)
		return objects

	def _construct_graph(self, services=None):
		'''
		Construct bonobo.Graph object(s) for the entire pipeline.
		'''
		g = bonobo.Graph()

		contents_records = g.add_chain(
			MatchingFiles(path='/', pattern=self.files_pattern, fs='fs.data.knoedler'),
			CurriedCSVReader(fs='fs.data.knoedler', limit=self.limit, field_names=self.headers),
		)
		sales = self.add_sales_chain(g, contents_records, services, serialize=True)
		self.add_transaction_chains(g, sales, services, serialize=True)

		self.graph = g
		return sales

	def get_graph(self, **kwargs):
		'''Return a single bonobo.Graph object for the entire pipeline.'''
		if not self.graph:
			self._construct_graph(**kwargs)

		return self.graph

	def run(self, services=None, **options):
		'''Run the Knoedler bonobo pipeline.'''
		if self.verbose:
			print(f'- Limiting to {self.limit} records per file', file=sys.stderr)
		if not services:
			services = self.get_services(**options)

		if self.verbose:
			print('Running graph...', file=sys.stderr)
		graph = self.get_graph(services=services, **options)
		self.run_graph(graph, services=services)

		if self.verbose:
			print('Serializing static instances...', file=sys.stderr)
		
		for model, instances in self.static_instances.used_instances().items():
			g = bonobo.Graph()
			nodes = self.serializer_nodes_for_model(model=self.models[model], use_memory_writer=False)
			values = instances.values()
			source = g.add_chain(GraphListSource(values))
			self.add_serialization_chain(g, source.output, model=self.models[model], use_memory_writer=False)
			self.run_graph(g, services={})


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
