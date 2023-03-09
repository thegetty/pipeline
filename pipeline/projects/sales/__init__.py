'''
Classes and utility functions for instantiating, configuring, and
running a bonobo pipeline for converting Sales Index CSV data into JSON-LD.
'''

# PIR Extracters

import random
import objgraph
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
from fractions import Fraction

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
from pipeline.projects import PipelineBase, UtilityHelper, PersonIdentity
from pipeline.projects.sales.util import *
from pipeline.util import \
			GraphListSource, \
			CaseFoldingSet, \
			CromObjectMerger, \
			RecursiveExtractKeyedValue, \
			ExtractKeyedValue, \
			ExtractKeyedValues, \
			MatchingFiles, \
			identity, \
			replace_key_pattern, \
			strip_key_prefix
from pipeline.io.file import MergingFileWriter
from pipeline.io.memory import MergingMemoryWriter
# from pipeline.io.arches import ArchesWriter
import pipeline.linkedart
from pipeline.linkedart import add_crom_data, get_crom_object
from pipeline.io.csv import CurriedCSVReader
from pipeline.nodes.basic import \
			RecordCounter, \
			KeyManagement, \
			PreserveCSVFields, \
			RemoveKeys, \
			GroupRepeatingKeys, \
			GroupKeys, \
			AddArchesModel, \
			Serializer, \
			OnlyRecordsOfType, \
			Trace
from pipeline.nodes.basic import AddFieldNamesSimple as AddFieldNames
from pipeline.util.rewriting import rewrite_output_files, JSONValueRewriter
import pipeline.projects.sales.events
import pipeline.projects.sales.lots
import pipeline.projects.sales.objects
import pipeline.projects.sales.catalogs

#mark - utility functions and classes

class SalesPersonIdentity(PersonIdentity):
	pass

class SalesUtilityHelper(UtilityHelper):
	'''
	Project-specific code for accessing and interpreting sales data.
	'''
	def __init__(self, project_name):
		super().__init__(project_name)
		# TODO: does this handle all the cases of data packed into the lot_number string that need to be stripped?
		self.shared_lot_number_re = re.compile(r'(\[[a-z]\])')
		self.ignore_house_authnames = CaseFoldingSet(('Anonymous', '[Anonymous]'))
		self.csv_source_columns = ['pi_record_no', 'star_record_no', 'catalog_number']
		self.problematic_record_uri = f'https://data.getty.edu/local/thesaurus/problematic-record'
		self.person_identity = SalesPersonIdentity(make_shared_uri=self.make_shared_uri, make_proj_uri=self.make_proj_uri)
		self.uid_tag_prefix = UID_TAG_PREFIX

	def copy_source_information(self, dst: dict, src: dict):
		for k in self.csv_source_columns:
			with suppress(KeyError):
				dst[k] = src[k]
		return dst

	def add_person(self, data, **kwargs):
		if data.get('name_so'):
			# handling of the name_so field happens here and not in the SalesPersonIdentity methods,
			# because it requires access to the services data on catalogs
			source = data.get('name_so', '').strip()
			components = source.split(' ')
			if len(components) == 2:
				owner_code, copy_number = components
			else:
				owner_code = source
				copy_number = ''
			cno = kwargs['catalog_number']
			owner_uri = self.physical_catalog_uri(cno, owner_code, None)
			copy_uri = self.physical_catalog_uri(cno, owner_code, copy_number)
			unique_catalogs = self.services['unique_catalogs']
			owned_copies = unique_catalogs.get(owner_uri)
			if owned_copies:
				if copy_uri in owned_copies:
					data['_name_source_catalog_key'] = (cno, owner_code, copy_number)
				else:
					warnings.warn(f'*** SPECIFIC PHYSICAL CATALOG COPY NOT FOUND FOR NAME SOURCE {source} in catalog {cno}')
			else:
				warnings.warn(f'*** NO CATALOG OWNER FOUND FOR NAME SOURCE {source} on catalog {cno}')
		return super().add_person(data, **kwargs)

	def event_type_for_sale_type(self, sale_type):
		if sale_type in ('Private Contract Sale', 'Stock List'):
			# 'Stock List' is treated just like a Private Contract Sale, except for the catalogs
			return vocab.Exhibition
		elif sale_type == 'Lottery':
			return vocab.Lottery
		elif sale_type in ('Auction', 'Collection Catalog'):
			return vocab.AuctionEvent
		else:
			warnings.warn(f'*** Unexpected sale type: {sale_type!r}')

	def sale_type_for_sale_type(self, sale_type):
		if sale_type in ('Private Contract Sale', 'Stock List'):
			# 'Stock List' is treated just like a Private Contract Sale, except for the catalogs
			return vocab.Negotiating
		elif sale_type == 'Lottery':
			return vocab.LotteryDrawing
		elif sale_type in ('Auction', 'Collection Catalog'):
			return vocab.Auction
		else:
			warnings.warn(f'*** Unexpected sale type: {sale_type!r}')

	def set_type_name_for_sale_type(self, sale_type):
		if sale_type in ('Private Contract Sale', 'Stock List', 'Collection Catalog'):
			return 'Object Set'
		elif sale_type in ('Auction', 'Lottery'):
			return 'Lot'
		else:
			warnings.warn(f'*** Unexpected sale type: {sale_type!r}')

	def catalog_type_for_sale_type(self, sale_type):
		if sale_type == 'Private Contract Sale':
			return vocab.ExhibitionCatalog
		elif sale_type == 'Stock List':
			return vocab.AccessionCatalog
		elif sale_type == 'Lottery':
			return vocab.LotteryCatalog
		elif sale_type in ('Auction', 'Collection Catalog'):
			return vocab.AuctionCatalog
		else:
			warnings.warn(f'*** Unexpected sale type: {sale_type!r}')

	def catalog_text(self, cno, sale_type='Auction'):
		uri = self.make_proj_uri('CATALOG', cno)
		label = f'Sale Catalog {cno}'

		if sale_type in ('Auction', 'Collection Catalog'): # Sale Catalog
			cl = vocab.AuctionCatalogText
		elif sale_type == 'Private Contract Sale': # Private Sale Exhibition Catalog
			cl = vocab.ExhibitionCatalogText
		elif sale_type == 'Stock List': # Accession Catalog
			cl = vocab.AccessionCatalogText
		elif sale_type == 'Lottery': # Lottery Catalog
			cl = vocab.LotteryCatalogText
		else:
			cl = vocab.SalesCatalogText # Sale Catalog

		catalog = vocab.make_multitype_obj(cl, vocab.CatalogForm, ident=uri, label=label)
		catalog.identified_by = model.Name(ident='', content=label)

		return catalog

	def catalog_type(self, cno, sale_type='Auction'):
		if sale_type in ('Auction', 'Collection Catalog'): # Sale Catalog
			cl = vocab.AuctionCatalogText
		elif sale_type == 'Private Contract Sale': # Private Sale Exhibition Catalog
			cl = vocab.ExhibitionCatalogText
		elif sale_type == 'Stock List': # Accession Catalog
			cl = vocab.AccessionCatalogText
		elif sale_type == 'Lottery': # Lottery Catalog
			cl = vocab.LotteryCatalogText
		else:
			cl = vocab.SalesCatalogText # Sale Catalog

		return cl

	def physical_catalog_notes(self, cno, owner, copy):
		cat_uri = self.physical_catalog_uri(cno, owner, copy)
		uri = cat_uri + '-HandNotes'
		labels = []
		if owner:
			labels.append(f'owned by “{owner}”')
		if copy:
			labels.append(f'copy {copy}')
		phys_label = ', '.join(labels)
		label = f'Handwritten notes in Catalog {cno}'
		catalog = model.LinguisticObject(ident=uri, label=label)
		return catalog

	def physical_catalog_uri(self, cno, owner=None, copy=None):
		keys = [v for v in [cno, owner, copy] if v]
		uri = self.make_proj_uri('PHYS-CAT', *keys)
		return uri

	def physical_catalog_label(self, cno, sale_type, owner=None, copy=None):
		labels = []
		if owner:
			labels.append(f'owned by “{owner}”')
		if copy:
			labels.append(f'copy {copy}')
		if sale_type in ('Auction', 'Collection Catalog'):
			labels = [f'Sale Catalog {cno}'] + labels
		elif sale_type == 'Private Contract Sale':
			labels = [f'Private Sale Exhibition Catalog {cno}'] + labels
		elif sale_type == 'Stock List':
			labels = [f'Stock List {cno}'] + labels
		elif sale_type == 'Lottery':
			labels = [f'Lottery Catalog {cno}'] + labels
		else:
			warnings.warn(f'*** Unexpected sale type: {sale_type!r}')
			return None
		label = ', '.join(labels)
		return label
		
	def physical_catalog(self, cno, sale_type, owner=None, copy=None, add_name=False):
		uri = self.physical_catalog_uri(cno, owner, copy)
		label = self.physical_catalog_label(cno, sale_type, owner, copy)
		catalog_type = self.catalog_type_for_sale_type(sale_type)
		catalog = catalog_type(ident=uri, label=label)
		if add_name:
			catalog.identified_by = vocab.Name(ident='', content=label)
		return catalog

	def sale_for_sale_type(self, sale_type, lot_object_key):
		cno, lno, date = lot_object_key
		uid, uri = self.shared_lot_number_ids(cno, lno, date)
		shared_lot_number = self.shared_lot_number_from_lno(lno)

		lot_type = self.sale_type_for_sale_type(sale_type)
		lot = lot_type(ident=uri)

		if sale_type in ('Auction', 'Collection Catalog'):
			lot_id = f'{cno} {shared_lot_number} ({date})'
			lot_label = f'Auction of Lot {lot_id}'
		elif sale_type in ('Private Contract Sale', 'Stock List'):
			lot_id = f'{cno} {shared_lot_number} ({date})'
			lot_label = f'Sale of Object Set {lot_id}'
		elif sale_type == 'Lottery':
			lot_id = f'{cno} {shared_lot_number} ({date})'
			lot_label = f'Lottery Drawing of Lot {lot_id}'
		else:
			warnings.warn(f'*** Unexpected sale type: {sale_type!r}')
		if lot_label:
			lot._label = lot_label
			lot.identified_by = model.Name(ident='', content=lot_label)
		return lot

	def sale_event_for_catalog_number(self, catalog_number, sale_type='Auction', date_label=None):
		'''
		Return a `vocab.AuctionEvent` object and its associated 'uid' key and URI, based on
		the supplied `catalog_number`.
		'''
		if sale_type == '':
			sale_type = 'Auction'

		event_type = self.event_type_for_sale_type(sale_type)
		sale_type_key = sale_type.replace(' ', '_').upper()
		uid = f'{sale_type_key}-EVENT-{catalog_number}'
		uri = self.make_proj_uri(f'{sale_type_key}-EVENT', catalog_number)
		label = f"{sale_type} Event {catalog_number}"
		if date_label:
			label += f' ({date_label})'
		auction = event_type(ident=uri, label=label)
		return auction, uid, uri

	def shared_lot_number_from_lno(self, lno):
		'''
		Given a `lot_number` value which identifies an object in a group, strip out the
		object-specific content, returning an identifier for the entire lot.

		For example, strip the object identifier suffixes such as '[a]':

		'0001[a]' -> '0001'
		'''
		m = self.shared_lot_number_re.search(lno)
		if m:
			return lno.replace(m.group(1), '')
		return lno

	def transaction_uri_for_lot(self, data, metadata):
		'''
		Return a URI representing the procurement which the object (identified by the
		supplied data) is a part of. This may identify just the lot being sold or, in the
		case of multiple lots being bought for a single price, a single procurement that
		encompasses multiple acquisitions that span different lots.
		'''
		prices = metadata.get('price', [])
		cno, lno, date = object_key(data)
		shared_lot_number = self.shared_lot_number_from_lno(lno)
		for p in prices:
			n = p.get('note')
			if n and n.startswith('for lots '):
				lot_list = n[9:].split(' & ')
				return self.make_proj_uri('PROV-MULTI', cno, date, *lot_list)
		return self.make_proj_uri('PROV', cno, date, shared_lot_number)

	def lots_in_transaction(self, data, metadata):
		'''
		Return a string that represents the lot numbers that are a part of the procurement
		related to the supplied data.
		'''
		prices = metadata.get('price', [])
		_, lno, _ = object_key(data)
		shared_lot_number = self.shared_lot_number_from_lno(lno)
		for p in prices:
			n = p.get('note')
			if n and n.startswith('for lots '):
				return n[9:]
		return shared_lot_number

	def shared_lot_number_ids(self, cno, lno, date, sale_type='Auction'):
		'''
		Return a tuple of a UID string and a URI for the lot identified by the supplied
		data which identifies a specific object in that lot.
		'''
		shared_lot_number = self.shared_lot_number_from_lno(lno)
		uid = f'AUCTION-{cno}-{shared_lot_number}-{date}'
		uri = self.make_proj_uri('AUCTION', cno, shared_lot_number, date)
		return uid, uri

	@staticmethod
	def transaction_contains_multiple_lots(data, metadata):
		'''
		Return `True` if the procurement related to the supplied data represents a
		transaction of multiple lots with a single payment, `False` otherwise.
		'''
		prices = metadata.get('price', [])
		for p in prices:
			n = p.get('note')
			if n and n.startswith('for lots '):
				return True
		return False

	def auction_house_uri(self, data:dict, sequence=1):
		key = self.auction_house_uri_keys(data, sequence=sequence)
		type = key[1]
		if type in ('ULAN', 'AUTH'):
			return self.make_shared_uri(*key)
		else:
			return self.make_proj_uri(*key)

	def auction_house_uri_keys(self, data:dict, sequence=1):
		ulan = None
		with suppress(ValueError, TypeError):
			ulan = int(data.get('ulan'))
		auth_name = data.get('auth')
		if ulan:
			return ('HOUSE', 'ULAN', ulan)
		elif auth_name and auth_name not in self.ignore_house_authnames:
			return ('HOUSE', 'AUTH', auth_name)
		else:
			# not enough information to identify this house uniquely, so use the source location in the input file
			if 'pi_record_no' in data:
				return ('HOUSE', 'PI', data['pi_record_no'], sequence)
			else:
				return ('HOUSE', 'STAR', data['star_record_no'], sequence)

	def add_auction_house_data(self, a:dict, sequence=1, event_record=None):
		'''Add modeling data for an auction house organization.'''
		catalog = a.get('_catalog')

		if 'uri' not in a:
			auction_house_uri_keys = self.auction_house_uri_keys(a, sequence=sequence)
			a['uri'] = self.auction_house_uri(a, sequence=sequence)
			a['uid'] = '-'.join([str(k) for k in auction_house_uri_keys])

		ulan = None
		with suppress(ValueError, TypeError):
			ulan = int(a.get('ulan'))
		auth_name = a.get('auth_name', a.get('auth'))
		a['identifiers'] = []
		if ulan:
			a['ulan'] = ulan
		elif auth_name and auth_name not in self.ignore_house_authnames:
			pname = vocab.PrimaryName(ident='', content=auth_name)
			if event_record:
				pname.referred_to_by = event_record
			a['identifiers'].append(pname)
			a['label'] = auth_name
			name = a.get('name')
			if name and name == auth_name:
				del a['name']

		name = a.get('name')
		if name and name != auth_name:
			n = model.Name(ident='', content=name)
			if event_record:
				n.referred_to_by = event_record
			a['identifiers'].append(n)
			a.setdefault('label', name)
		else:
			a.setdefault('label', '(Anonymous)')

		make_house = pipeline.linkedart.MakeLinkedArtAuctionHouseOrganization()
		make_house(a)
		house = get_crom_object(a)

		return add_crom_data(data=a, what=house)

	def lot_number_identifier(self, lno, cno, non_auctions, sale_type, **kwargs):
		'''
		Return an Identifier for the lot number that is classified as a LotNumber,
		and whose assignment has the specific purpose of the auction event.
		'''
		sale_type = non_auctions.get(cno, 'Auction')
		auction, _, _ = self.sale_event_for_catalog_number(cno, sale_type, **kwargs)
		lot_number = vocab.LotNumber(ident='', content=lno)
		assignment = model.AttributeAssignment(ident='', label=f'Assignment of lot number {lno} from {cno}')
		assignment.specific_purpose = auction
		lot_number.assigned_by = assignment
		return lot_number


def add_crom_price(data, parent, services, add_citations=False):
	'''
	Add modeling data for `MonetaryAmount`, `StartingPrice`, or `EstimatedPrice`,
	based on properties of the supplied `data` dict.
	'''
	currencies = services['currencies']
	decimalization = services['currencies_decimalization']
	region_currencies = services['region_currencies']
	cno = parent['catalog_number']
	region, _ = cno.split('-', 1)
	c = currencies.copy()
	if region in region_currencies:
		c.update(region_currencies[region])

	verbatim = []
	for k in ('price', 'est_price', 'start_price', 'ask_price'):
		# Each data record can only have one of these. We put the decimalized
		# value back using the same key, but the verbatim strings are just
		# associated with the MonetaryAmount object, regardless of the presence
		# of any classification (estimated/starting/asking)
		if k in data:
			price = data.get(k)
			if '-' in price:
				with suppress(ValueError, KeyError):
					price = price.replace('[?]', '').strip()
					currency = data['currency']
					currency = c.get(currency.lower(), currency)
					parts = [int(v) for v in price.split('-')]
					if currency in decimalization:
						decimalization_data = decimalization[currency]
						primary_unit = decimalization_data['primary_unit']
						primary_value = int(parts.pop(0))
						total_price = Fraction(primary_value)
						part_names = [f'{primary_value} {primary_unit}']
						denom = 1
						for value, unit in zip(parts, decimalization_data['subunits']):
							if value:
								name = unit[0]
								denom = denom * unit[1]
								frac = Fraction(value, denom)
								total_price += frac
								part_names.append(f'{value} {name}')
						decimalized_value = str(float(total_price))
						verbatim.append(price)
					else:
						decimalized_value = price
						warnings.warn(f'No decimalization rules for currency {currency!r}')
						verbatim.append(price)
					# handle decimalization of £sd price, and preserve the original value in verbatim
					data[k] = decimalized_value

	amnt = extract_monetary_amount(data, currency_mapping=c, add_citations=add_citations)
	if amnt:
		for v in verbatim:
			amnt.identified_by = model.Name(ident='', content=v)
		add_crom_data(data=data, what=amnt)

	return data


#mark - Sales Pipeline class

class SalesPipeline(PipelineBase):
	'''Bonobo-based pipeline for transforming Sales data from CSV into JSON-LD.'''
	def __init__(self, input_path, catalogs, auction_events, contents, **kwargs):
		project_name = 'sales'
		self.input_path = input_path
		self.services = None

		helper = SalesUtilityHelper(project_name)
		self.uid_tag_prefix = UID_TAG_PREFIX

		vocab.register_instance('act of selling', {'parent': model.Type, 'id': '300438483', 'label': 'Act of Selling'})
		vocab.register_instance('act of returning', {'parent': model.Type, 'id': '300438467', 'label': 'Returning'})
		vocab.register_instance('act of completing sale', {'parent': model.Type, 'id': '300448858', 'label': 'Act of Completing Sale'})
		vocab.register_instance('qualifier', {'parent': model.Type, 'id': '300435720', 'label': 'Qualifier'})
		vocab.register_instance('form type', {'parent': model.Type, 'id': '300444970', 'label': 'Form'})		
		vocab.register_instance('buyer description', {'parent': model.Type, 'id': '300445024', 'label': 'Buyer description'})
		vocab.register_instance('seller description', {'parent': model.Type, 'id': '300445025', 'label': 'Seller description'})
		
		vocab.register_instance('fire', {'parent': model.Type, 'id': '300068986', 'label': 'Fire'})
		vocab.register_instance('animal', {'parent': model.Type, 'id': '300249395', 'label': 'Animal'})
		vocab.register_instance('history', {'parent': model.Type, 'id': '300033898', 'label': 'History'})

		vocab.register_vocab_class('UncertainMemberClosedGroup', {'parent': model.Group, 'id': '300448855', 'label': 'Closed Group Representing an Uncertain Person'})
		vocab.register_vocab_class('ConstructedTitle', {'parent': model.Name, 'id': '300417205', 'label': 'Constructed Title'})
		vocab.register_vocab_class('AuctionHouseActivity', {'parent': model.Activity, 'id': '300417515', 'label': 'Auction House'})

		vocab.register_vocab_class('EntryNumber', {"parent": model.Identifier, "id":"300445023", "label": "Entry Number"})
		vocab.register_vocab_class('PageNumber', {"parent": model.Identifier, "id":"300445022", "label": "Page Number"})
		vocab.register_vocab_class('OrderNumber', {"parent": model.Identifier, "id":"300247348", "label": "Order"})
		
		vocab.register_vocab_class('BookNumber', {"parent": model.Identifier, "id":"300445021", "label": "Book Number"})
		vocab.register_vocab_class('PageTextForm', {"parent": model.LinguisticObject, "id":"300194222", "label": "Page", "metatype": "form type"})
		vocab.register_vocab_class('EntryTextForm', {"parent": model.LinguisticObject, "id":"300438434", "label": "Entry", "metatype": "form type"})

		vocab.register_vocab_class('CatalogForm', {"parent": model.LinguisticObject, "id":"300026059", "label": "Catalog", "metatype": "form type"})

		vocab.register_vocab_class('SalePrice', {"parent": model.MonetaryAmount, "id":"300417246", "label": "Sale Price"})
				
		super().__init__(project_name, helper=helper)

		self.graph_0 = None
		self.graph_1 = None
		self.graph_2 = None
		self.graph_3 = None
		self.models = kwargs.get('models', settings.arches_models)
		self.catalogs_header_file = catalogs['header_file']
		self.catalogs_files_pattern = catalogs['files_pattern']
		self.auction_events_header_file = auction_events['header_file']
		self.auction_events_files_pattern = auction_events['files_pattern']
		self.contents_header_file = contents['header_file']
		self.contents_files_pattern = contents['files_pattern']
		self.limit = kwargs.get('limit')
		self.debug = kwargs.get('debug', False)

		fs = bonobo.open_fs(input_path)
		with fs.open(self.catalogs_header_file, newline='') as csvfile:
			r = csv.reader(csvfile)
			self.catalogs_headers = [v.lower() for v in next(r)]
		with fs.open(self.auction_events_header_file, newline='') as csvfile:
			r = csv.reader(csvfile)
			self.auction_events_headers = [v.lower() for v in next(r)]
		with fs.open(self.contents_header_file, newline='') as csvfile:
			r = csv.reader(csvfile)
			self.contents_headers = [v.lower() for v in next(r)]

	def setup_services(self):
	# Set up environment
		'''Return a `dict` of named services available to the bonobo pipeline.'''
		services = super().setup_services()

		# make these case-insensitive by wrapping the value lists in CaseFoldingSet
		for name in ('transaction_types', 'attribution_modifiers', 'date_modifiers'):
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
			'make_la_person': pipeline.linkedart.MakeLinkedArtPerson(),
			'unique_catalogs': defaultdict(set),
			'post_sale_map': {},
			'event_properties': {
				'auction_houses': defaultdict(list),
				'auction_dates': {},
				'auction_date_label': {},
				'auction_locations': {},
				'experts': defaultdict(list),
				'commissaire': defaultdict(list),
			},
			'non_auctions': {},
			'counts': defaultdict(int)
		})
		return services

	def add_physical_catalogs_chain(self, graph, records, serialize=True):
		'''Add modeling of physical copies of auction catalogs.'''
		catalogs = graph.add_chain(
			PreserveCSVFields(key='star_csv_data', order=self.catalogs_headers),
			pipeline.projects.sales.catalogs.AddPhysicalCatalogEntry(helper=self.helper),
			pipeline.projects.sales.catalogs.AddAuctionCatalog(helper=self.helper),
			pipeline.projects.sales.catalogs.AddPhysicalCatalogObjects(helper=self.helper),
			pipeline.projects.sales.catalogs.AddPhysicalCatalogOwners(helper=self.helper),
			RecordCounter(name='physical_catalogs', verbose=self.debug),
			_input=records.output
		)
		records = graph.add_chain(
			ExtractKeyedValue(key='_catalog_record'),
			_input=catalogs.output
		)
		if serialize:
			# write SALES data
			self.add_serialization_chain(graph, catalogs.output, model=self.models['HumanMadeObject'], use_memory_writer=False)
			self.add_serialization_chain(graph, records.output, model=self.models['LinguisticObject'], use_memory_writer=False)
		return catalogs

	def add_catalog_linguistic_objects_chain(self, graph, events, serialize=True):
		'''Add modeling of auction catalogs as linguistic objects.'''
		los = graph.add_chain(
			ExtractKeyedValue(key='_catalog'),
			pipeline.projects.sales.catalogs.PopulateAuctionCatalog(helper=self.helper, static_instances=self.static_instances),
			_input=events.output
		)
		if serialize:
			# write SALES data
			self.add_serialization_chain(graph, los.output, model=self.models['LinguisticObject'], use_memory_writer=False)
		return los

	def add_auction_events_chain(self, graph, records, serialize=True):
		'''Add modeling of auction events.'''
		auction_events = graph.add_chain(
			PreserveCSVFields(key='star_csv_data', order=self.auction_events_headers),
			KeyManagement(
				drop_empty=True,
				operations=[
					{
						'group_repeating': {
							'seller': {'prefixes': ('sell_auth_name', 'sell_auth_q')},
							'expert': {
								'rename_keys': {
									'expert': 'name',
									'expert_auth': 'auth_name',
									'expert_ulan': 'ulan'
								},
# 								'postprocess': [
# 									lambda x, _: replace_key_pattern(r'^(expert)$', 'expert_name', x),
# 									lambda x, _: strip_key_prefix('expert_', x),
# 									lambda x, _: replace_key_pattern(r'^(auth)$', 'auth_name', x),
# 								],
								'prefixes': ('expert', 'expert_auth', 'expert_ulan')
							},
							'commissaire': {
								'rename_keys': {
									'comm_pr': 'name',
									'comm_pr_auth': 'auth_name',
									'comm_pr_ulan': 'ulan'
								},
# 								'postprocess': [
# 									lambda x, _: replace_key_pattern(r'^(comm_pr)$', 'comm_pr_name', x),
# 									lambda x, _: strip_key_prefix('comm_pr_', x),
# 									lambda x, _: replace_key_pattern(r'^(auth)$', 'auth_name', x),
# 								],
								'prefixes': ('comm_pr', 'comm_pr_auth', 'comm_pr_ulan')
							},
							'auction_house': {
								'rename_keys': {
									'auc_house_name': 'name',
									'auc_house_auth': 'auth_name',
									'auc_house_ulan': 'ulan'
								},
# 								'postprocess': [
# 									lambda x, _: strip_key_prefix('auc_house_', x),
# 								],
								'prefixes': ('auc_house_name', 'auc_house_auth', 'auc_house_ulan')
							},
							'portal': {'prefixes': ('portal_url',)},
						},
						'group': {
							'lugt': {'properties': ('lugt_number_1', 'lugt_number_2', 'lugt_number_3')},
							'auc_copy': {
								'properties': (
									'auc_copy_seller_1',
									'auc_copy_seller_2',
									'auc_copy_seller_3',
									'auc_copy_seller_4')},
							'other_seller': {
								'properties': (
									'other_seller_1',
									'other_seller_2',
									'other_seller_3')},
							'title_pg_sell': {'properties': ('title_pg_sell_1', 'title_pg_sell_2')},
							'links': {
								'properties': (
									'hoet_link',
									'art_sales_cats_online',
									'art_world_in_britain',
									'portal'
								)
							},
							'location': {
								'properties': (
									'city_of_sale',
									'sale_location',
									'country_auth',
									'specific_loc')},
						}
					}
				]
			),
			pipeline.projects.sales.catalogs.AddAuctionCatalog(helper=self.helper),
			pipeline.projects.sales.events.AddAuctionEvent(helper=self.helper),
			pipeline.projects.sales.events.AddAuctionHouses(helper=self.helper),
			pipeline.projects.sales.events.PopulateAuctionEvent(helper=self.helper),
			RecordCounter(name='auction_events', verbose=self.debug),
			_input=records.output
		)
		if serialize:
			# write SALES data
			self.add_serialization_chain(graph, auction_events.output, model=self.models['SaleActivity'], use_memory_writer=False)
		return auction_events

	def add_procurement_chain(self, graph, acquisitions, serialize=True):
		'''Add modeling of the procurement event of an auction of a lot.'''
		p = graph.add_chain(
			ExtractKeyedValues(key='_prov_entries'),
			_input=acquisitions.output
		)
		if serialize:
			# write SALES data
			self.add_serialization_chain(graph, p.output, model=self.models['ProvenanceEntry'], use_memory_writer=False)

	def add_buyers_sellers_chain(self, graph, acquisitions, serialize=True):
		'''Add modeling of the buyers, bidders, and sellers involved in an auction.'''
		buyers = self.add_person_or_group_chain(graph, acquisitions, key='buyer', serialize=serialize)
		sellers = self.add_person_or_group_chain(graph, acquisitions, key='seller', serialize=serialize)
		owners = self.add_person_or_group_chain(graph, acquisitions, key='_other_owners', serialize=serialize)

	def add_acquisitions_chain(self, graph, sales, serialize=True):
		'''Add modeling of the acquisitions and bidding on lots being auctioned.'''
		bid_acqs = graph.add_chain(
			pipeline.projects.sales.lots.AddAcquisitionOrBidding(helper=self.helper),
			_input=sales.output
		)

		orgs = self.add_person_or_group_chain(graph, bid_acqs, key='_organizations', serialize=serialize)
		refs = graph.add_chain(
			ExtractKeyedValues(key='_citation_references'),
			_input=bid_acqs.output
		)
		acqs = graph.add_chain(
			ExtractKeyedValue(key='_acquisition'),
			_input=bid_acqs.output
		)
		drawing = graph.add_chain(
			ExtractKeyedValue(key='_drawing'),
			_input=bid_acqs.output
		)
		notes = graph.add_chain(
			ExtractKeyedValues(key='_phys_catalog_notes'),
			_input=bid_acqs.output
		)
		catalogs = graph.add_chain(
			ExtractKeyedValues(key='_phys_catalogs'),
			_input=bid_acqs.output
		)
		_ = self.add_places_chain(graph, bid_acqs, key='_owner_locations', serialize=True)

		if serialize:
			# write SALES data
			self.add_serialization_chain(graph, catalogs.output, model=self.models['HumanMadeObject'])
			self.add_serialization_chain(graph, notes.output, model=self.models['LinguisticObject'], use_memory_writer=False)
			self.add_serialization_chain(graph, refs.output, model=self.models['LinguisticObject'], use_memory_writer=False)
			self.add_serialization_chain(graph, drawing.output, model=self.models['SaleActivity'], use_memory_writer=False)
		return bid_acqs

	def add_sales_chain(self, graph, records, services, serialize=True):
		'''Add transformation of sales records to the bonobo pipeline.'''
		sales = graph.add_chain(
			PreserveCSVFields(key='star_csv_data', order=self.contents_headers),
			KeyManagement(
				drop_empty=True,
				operations=[
					{
						'remove': {
							'expert_auth_1', 'expert_ulan_1', 'expert_auth_2', 'expert_ulan_2', 'expert_auth_3', 'expert_ulan_3', 'expert_auth_4', 'expert_ulan_4',
							'commissaire_pr_1', 'comm_ulan_1', 'commissaire_pr_2', 'comm_ulan_2', 'commissaire_pr_3', 'comm_ulan_3', 'commissaire_pr_4', 'comm_ulan_4',
							'auction_house_1', 'house_ulan_1', 'auction_house_2', 'house_ulan_2', 'auction_house_3', 'house_ulan_3', 'auction_house_4', 'house_ulan_4',
						},
						'group_repeating': {
							'expert': {'prefixes': ('expert_auth', 'expert_ulan')},
							'commissaire': {'prefixes': ('commissaire_pr', 'comm_ulan')},
							'auction_house': {
								'rename_keys': {
									'auction_house': 'name',
									'house_ulan': 'ulan'
								},
# 								'postprocess': [
# 									lambda x, _: replace_key_pattern(r'(auction_house)', 'house_name', x),
# 									lambda x, _: strip_key_prefix('house_', x),
# 								],
								'prefixes': ('auction_house', 'house_ulan')
							},
							'_artists': {
								'rename_keys': {
									'artist_info': 'biography',
									'art_authority': 'auth_name'
								},
								'postprocess': [
									filter_empty_person,
									add_pir_record_ids
								],
								'prefixes': (
									'artist_name', 'art_authority',
									'artist_info', 'nationality', 'artist_ulan',
									'attrib_mod', 'attrib_mod_auth',
								)
							},
							'hand_note': {'prefixes': ('hand_note', 'hand_note_so')},
							'seller': {
								'rename_keys': {
									'sell_name': 'name',
									'sell_name_so': 'so',
									'sell_name_ques': 'ques',
									'sell_mod': 'mod',
									'sell_auth_mod': 'auth_mod',
									'sell_auth_mod_a': 'auth_mod_a',
									'sell_auth_name': 'auth_name',
									'sell_auth_nameq': 'auth_nameq',
									'sell_ulan': 'ulan'
								},
								'postprocess': [
# 									lambda x, _: strip_key_prefix('sell_', x),
									filter_empty_person
								],
								'prefixes': (
									'sell_name',
									'sell_name_so',
									'sell_name_ques',
									'sell_mod',
									'sell_auth_mod',
									'sell_auth_mod_a',
									'sell_auth_name',
									'sell_auth_nameq',
									'sell_ulan'
								)
							},
							'price': {
								'rename_keys': {
									'price_amount': 'price',
									'price_amount_q': 'uncertain',
									'price_currency': 'currency',
									'price_note': 'note',
									'price_source': 'source',
									'price_citation': 'citation',
								},
								'postprocess': lambda d, p: add_crom_price(d, p, services, add_citations=True),
								'prefixes': (
									'price_amount',
									'price_amount_q',
									'price_currency',
									'price_note',
									'price_source',
									'price_citation')},
							'buyer': {
								'rename_keys': {
									'buy_name': 'name',
									'buy_name_so': 'name_so',
									'buy_name_ques': 'name_ques',
									'buy_name_cite': 'name_cite',
									'buy_auth_name': 'auth_name',
									'buy_auth_nameq': 'auth_nameq',
									'buy_mod': 'mod',
									'buy_auth_mod': 'auth_mod',
									'buy_auth_mod_a': 'auth_mod_a',
									'buy_ulan': 'ulan'
								},
								'postprocess': [
# 									lambda x, _: strip_key_prefix('buy_', x),
									filter_empty_person
								],
								'prefixes': (
									'buy_name',
									'buy_name_so',
									'buy_name_ques',
									'buy_name_cite',
									'buy_auth_name',
									'buy_auth_nameq',
									'buy_mod',
									'buy_auth_mod',
									'buy_auth_mod_a',
									'buy_ulan'
								)
							},
							'prev_owner': {
								'rename_keys': {
									'prev_owner': 'name',
									'prev_own_ques': 'own_ques',
									'prev_own_so': 'own_so',
									'prev_own_auth': 'auth_name',
									'prev_own_auth_d': 'own_auth_d',
									'prev_own_auth_l': 'own_auth_l',
									'prev_own_auth_q': 'own_auth_q',
									'prev_own_auth_e': 'own_auth_e',
									'prev_own_auth_p': 'own_auth_p',
									'prev_own_ulan': 'own_ulan'
								},
# 								'postprocess': [
# 									lambda x, _: replace_key_pattern(r'(prev_owner)', 'prev_own', x),
# 									lambda x, _: strip_key_prefix('prev_', x),
# 								],
								'prefixes': (
									'prev_owner',
									'prev_own_ques',
									'prev_own_so',
									'prev_own_auth',
									'prev_own_auth_d',
									'prev_own_auth_l',
									'prev_own_auth_q',
									'prev_own_auth_e',
									'prev_own_auth_p',
									'prev_own_ulan'
								)
							},
							'other_titles': {
								'rename_keys': {
									'post_sale_ttl': 'title',
								},
								'prefixes': (
									'post_sale_ttl')},
							'prev_sale': {
								'rename_keys': {
									'prev_sale_year': 'year',
									'prev_sale_mo': 'mo',
									'prev_sale_day': 'day',
									'prev_sale_lot': 'lot',
									'prev_sale_loc': 'loc',
									'prev_sale_ques': 'ques',
									'prev_sale_cat': 'cat'
								},
# 								'postprocess': lambda x, _: strip_key_prefix('prev_sale_', x),
								'prefixes': (
									'prev_sale_year',
									'prev_sale_mo',
									'prev_sale_day',
									'prev_sale_lot',
									'prev_sale_loc',
									'prev_sale_ques',
									'prev_sale_cat'
								)
							},
							'post_sale': {
								'rename_keys': {
									'post_sale_year': 'year',
									'post_sale_mo': 'mo',
									'post_sale_day': 'day',
									'post_sale_lot': 'lot',
									'post_sale_loc': 'loc',
									'post_sale_q': 'q',
									'post_sale_art': 'art',
									'post_sale_nte': 'nte',
									'post_sale_col': 'col',
									'post_sale_cat': 'cat'
								},
# 								'postprocess': lambda x, _: strip_key_prefix('post_sale_', x),
								'prefixes': (
									'post_sale_year',
									'post_sale_mo',
									'post_sale_day',
									'post_sale_lot',
									'post_sale_loc',
									'post_sale_q',
									'post_sale_art',
									'post_sale_nte',
									'post_sale_col',
									'post_sale_cat'
								)
							},
							'post_owner': {
								'rename_keys': {
									'post_own': 'name',
									'post_own_q': 'own_q',
									'post_own_so': 'own_so',
									'post_own_auth': 'auth_name',
									'post_own_auth_d': 'own_auth_d',
									'post_own_auth_l': 'own_auth_l',
									'post_own_auth_q': 'own_auth_q',
									'post_own_auth_e': 'own_auth_e',
									'post_own_auth_p': 'own_auth_p',
									'post_own_ulan': 'own_ulan'
								},
# 								'postprocess': lambda x, _: strip_key_prefix('post_', x),
								'prefixes': (
									'post_own',
									'post_own_q',
									'post_own_so',
									'post_own_auth',
									'post_own_auth_d',
									'post_own_auth_l',
									'post_own_auth_q',
									'post_own_auth_e',
									'post_own_auth_p',
									'post_own_ulan'
								)
							},
							'portal': {'prefixes': ('portal_url',)},
							'present_location': {
								'rename_keys': {
									'pres_loc_geog': 'geog',
									'pres_loc_inst': 'inst',
									'pres_loc_insq': 'insq',
									'pres_loc_insi': 'insi',
									'pres_loc_acc': 'acc',
									'pres_loc_accq': 'accq',
									'pres_loc_note': 'note',
								},
								'prefixes': (
									'pres_loc_geog',
									'pres_loc_inst',
									'pres_loc_insq',
									'pres_loc_insi',
									'pres_loc_acc',
									'pres_loc_accq',
									'pres_loc_note',
								)
							}
						}
					},
					{
						'group': {
							'auction_of_lot': {
								'properties': (
									'link_to_pdf',
									'catalog_number',
									'lot_number',
									'lot_sale_year',
									'lot_sale_month',
									'lot_sale_day',
									'lot_sale_mod',
									'lot_notes')},
							'_object': {
								'postprocess': add_pir_object_uri_factory(self.helper),
								'properties': (
									'star_csv_data',
									'title',
									'other_titles',
									'title_modifier',
									'object_type',
									'materials',
									'dimensions',
									'formatted_dimens',
									'format',
									'genre',
									'subject',
									'inscription',
									'present_location',
									'_artists',
									'hand_note',
									'post_sale',
									'prev_sale',
									'prev_owner',
									'post_owner',
									'portal',
									'title_translation')},
							'estimated_price': {
								'rename_keys': {
									'est_price_q': 'uncertain',
									'est_price_curr': 'currency',
									'est_price_desc': 'note',
									'est_price_so': 'source',
								},
								'postprocess': lambda d, p: add_crom_price(d, p, services, add_citations=True),
								'properties': (
									'est_price',
									'est_price_q',
									'est_price_curr',
									'est_price_desc',
									'est_price_so')},
							'start_price': {
								'rename_keys': {
									'start_price_q': 'uncertain',
									'start_price_curr': 'currency',
									'start_price_desc': 'note',
									'start_price_so': 'source',
								},
								'postprocess': lambda d, p: add_crom_price(d, p, services, add_citations=True),
								'properties': (
									'start_price',
									'start_price_q',
									'start_price_curr',
									'start_price_desc',
									'start_price_so')},
							'ask_price': {
								'rename_keys': {
									'ask_price_q': 'uncertain',
									'ask_price_curr': 'currency',
									'ask_price_desc': 'note',
									'ask_price_so': 'source',
								},
								'postprocess': lambda d, p: add_crom_price(d, p, services, add_citations=True),
								'properties': (
									'ask_price',
									'ask_price_q',
									'ask_price_curr',
									'ask_price_desc',
									'ask_price_so')},
							'links': {
								'properties': (
									'portal',
									'link_to_pdf' # this is already grouped above in 'auction_of_lot'
								)
							}
						}
					}
				]
			),
			pipeline.projects.sales.catalogs.AddAuctionCatalogEntry(helper=self.helper),
			pipeline.projects.sales.lots.AddAuctionOfLot(helper=self.helper),
			_input=records.output
		)

		text = graph.add_chain(
			ExtractKeyedValue(key='_text_page'),
			_input=sales.output
		)

		auctions_of_lot = graph.add_chain(
			ExtractKeyedValue(key='_event_causing_prov_entry'),
			OnlyRecordsOfType(type=vocab.Auction),
			_input=sales.output
		)

		private_sale_activities = graph.add_chain(
			ExtractKeyedValue(key='_event_causing_prov_entry'),
			OnlyRecordsOfType(type=vocab.Negotiating),
			_input=sales.output
		)

		lottery_drawings = graph.add_chain(
			ExtractKeyedValue(key='_event_causing_prov_entry'),
			OnlyRecordsOfType(type=vocab.LotteryDrawing),
			_input=sales.output
		)

		if serialize:
			# write SALES data
			self.add_serialization_chain(graph, text.output, model=self.models['LinguisticObject'])
			self.add_serialization_chain(graph, auctions_of_lot.output, model=self.models['SaleActivity'])
			self.add_serialization_chain(graph, private_sale_activities.output, model=self.models['SaleActivity'])
			self.add_serialization_chain(graph, lottery_drawings.output, model=self.models['SaleActivity'])
		return sales

	def add_object_chain(self, graph, sales, serialize=True):
		'''Add modeling of the objects described by sales records.'''
		objects = graph.add_chain(
			ExtractKeyedValue(key='_object'),
			pipeline.projects.sales.objects.add_object_type,
			pipeline.projects.sales.objects.PopulateSalesObject(helper=self.helper),
			pipeline.linkedart.MakeLinkedArtHumanMadeObject(),
			pipeline.projects.sales.objects.AddArtists(helper=self.helper),
			RecordCounter(name='sales_records', verbose=self.debug),
			_input=sales.output
		)

		original_objects = graph.add_chain(
			ExtractKeyedValues(key='_original_objects'),
			_input=objects.output
		)

		events = graph.add_chain(
			ExtractKeyedValues(key='_events'),
			_input=objects.output
		)

		if serialize:
			# write OBJECTS data
			self.add_serialization_chain(graph, events.output, model=self.models['Event'])
			self.add_serialization_chain(graph, objects.output, model=self.models['HumanMadeObject'], use_memory_writer=False)
			self.add_serialization_chain(graph, original_objects.output, model=self.models['HumanMadeObject'], use_memory_writer=False)

		return objects

	def add_lot_set_chain(self, graph, objects, serialize=True):
		'''Add extraction and serialization of locations.'''
		sets = graph.add_chain(
			ExtractKeyedValue(key='_lot_object_set'),
			_input=objects.output
		)
		if serialize:
			# write SETS data
			self.add_serialization_chain(graph, sets.output, model=self.models['Set'])
		return sets

	def add_visual_item_chain(self, graph, objects, serialize=True):
		'''Add transformation of visual items to the bonobo pipeline.'''
		items = graph.add_chain(
			ExtractKeyedValue(key='_visual_item'),
			pipeline.linkedart.MakeLinkedArtRecord(),
			_input=objects.output
		)
		if serialize:
			# write VISUAL ITEMS data
			self.add_serialization_chain(graph, items.output, model=self.models['VisualItem'], use_memory_writer=False)
		return items

	def add_record_text_chain(self, graph, objects, serialize=True):
		'''Add transformation of record texts to the bonobo pipeline.'''
		texts = graph.add_chain(
			ExtractKeyedValue(key='_record'),
			pipeline.linkedart.MakeLinkedArtLinguisticObject(),
			_input=objects.output
		)
		if serialize:
			# write RECORD data
			self.add_serialization_chain(graph, texts.output, model=self.models['LinguisticObject'])
		return texts

	def add_texts_chain(self, graph, objects, serialize=True):
		texts = graph.add_chain(
			ExtractKeyedValues(key='_texts'),
			_input=objects.output
		)
		if serialize:
			# write RECORD data
			self.add_serialization_chain(graph, texts.output, model=self.models['LinguisticObject'])
		return texts

	def _construct_graph(self, single_graph=False, services=None):
		'''
		Construct bonobo.Graph object(s) for the entire pipeline.

		If `single_graph` is `False`, generate three `Graph`s (`self.graph_1`,
		`self.graph_2`, and `self.graph_3`), that will be run sequentially. The first for
		events, then catalogs, and finally for sales auctions (which depends on output from the first).

		If `single_graph` is `True`, then generate a single `Graph` that has the entire
		pipeline in it (`self.graph_0`). This is used to be able to produce graphviz
		output of the pipeline for visual inspection.
		'''
		graph0 = bonobo.Graph()
		graph1 = bonobo.Graph()
		graph2 = bonobo.Graph()
		graph3 = bonobo.Graph()

		component1 = [graph0] if single_graph else [graph1]
		component2 = [graph0] if single_graph else [graph2]
		component3 = [graph0] if single_graph else [graph3]
		for g in component1:
			auction_events_records = g.add_chain(
				MatchingFiles(path='/', pattern=self.auction_events_files_pattern, fs='fs.data.sales'),
				CurriedCSVReader(fs='fs.data.sales', limit=self.limit, field_names=self.auction_events_headers),
# 				AddFieldNames(field_names=self.auction_events_headers)
			)

			auction_events = self.add_auction_events_chain(g, auction_events_records, serialize=True)
			_ = self.add_catalog_linguistic_objects_chain(g, auction_events, serialize=True)
			_ = self.add_places_chain(g, auction_events, serialize=True)

			organizers = g.add_chain(
				ExtractKeyedValues(key='_organizers'),
				_input=auction_events.output
			)
			_ = self.add_person_or_group_chain(g, organizers, serialize=True)

		for g in component2:
			physical_catalog_records = g.add_chain(
				MatchingFiles(path='/', pattern=self.catalogs_files_pattern, fs='fs.data.sales'),
				CurriedCSVReader(fs='fs.data.sales', limit=self.limit, field_names=self.catalogs_headers),
# 				AddFieldNames(field_names=self.catalogs_headers),
			)

			catalogs = self.add_physical_catalogs_chain(g, physical_catalog_records, serialize=True)

			catalog_owners = g.add_chain(
				ExtractKeyedValue(key='_owner'),
				pipeline.linkedart.MakeLinkedArtAuctionHouseOrganization(),
				_input=catalogs.output
			)
			_ = self.add_person_or_group_chain(g, catalog_owners, serialize=True)

		for g in component3:
			contents_records = g.add_chain(
				MatchingFiles(path='/', pattern=self.contents_files_pattern, fs='fs.data.sales'),
				CurriedCSVReader(fs='fs.data.sales', limit=self.limit, field_names=self.contents_headers),
# 				AddFieldNames(field_names=self.contents_headers),
			)
			sales = self.add_sales_chain(g, contents_records, services, serialize=True)
			_ = self.add_lot_set_chain(g, sales, serialize=True)
			_ = self.add_texts_chain(g, sales, serialize=True)
			objects = self.add_object_chain(g, sales, serialize=True)
			_ = self.add_places_chain(g, objects, serialize=True)
			acquisitions = self.add_acquisitions_chain(g, objects, serialize=True)
			self.add_buyers_sellers_chain(g, acquisitions, serialize=True)
			self.add_procurement_chain(g, acquisitions, serialize=True)
			_ = self.add_person_or_group_chain(g, objects, key='_artists', serialize=True)
			_ = self.add_record_text_chain(g, objects, serialize=True)
			_ = self.add_visual_item_chain(g, objects, serialize=True)

		if single_graph:
			self.graph_0 = graph0
		else:
			self.graph_1 = graph1
			self.graph_2 = graph2
			self.graph_3 = graph3

	def get_graph(self, **kwargs):
		'''Return a single bonobo.Graph object for the entire pipeline.'''
		if not self.graph_0:
			self._construct_graph(single_graph=True, **kwargs)

		return self.graph_0

	def get_graph_1(self, **kwargs):
		'''Construct the bonobo pipeline to fully transform Sales data from CSV to JSON-LD.'''
		if not self.graph_1:
			self._construct_graph(**kwargs)
		return self.graph_1

	def get_graph_2(self, **kwargs):
		'''Construct the bonobo pipeline to fully transform Sales data from CSV to JSON-LD.'''
		if not self.graph_2:
			self._construct_graph(**kwargs)
		return self.graph_2

	def get_graph_3(self, **kwargs):
		'''Construct the bonobo pipeline to fully transform Sales data from CSV to JSON-LD.'''
		if not self.graph_3:
			self._construct_graph(**kwargs)
		return self.graph_3

	def checkpoint(self):
		pass

	def run(self, services=None, **options):
		'''Run the Sales bonobo pipeline.'''
		if self.verbose:
			print(f'- Limiting to {self.limit} records per file', file=sys.stderr)
		if not services:
			services = self.get_services(**options)

		if self.verbose:
			print('Running graph component 1...', file=sys.stderr)
		graph1 = self.get_graph_1(**options, services=services)
		self.run_graph(graph1, services=services)

		self.checkpoint()

		if self.verbose:
			print('Running graph component 2...', file=sys.stderr)
		graph2 = self.get_graph_2(**options, services=services)
		self.run_graph(graph2, services=services)

		self.checkpoint()

		if self.verbose:
			print('Running graph component 3...', file=sys.stderr)
		graph3 = self.get_graph_3(**options, services=services)
		self.run_graph(graph3, services=services)

		self.checkpoint()

		if self.verbose:
			print('Serializing static instances...', file=sys.stderr)
		for model, instances in self.static_instances.used_instances().items():
			g = bonobo.Graph()
			with suppress(KeyError):
				nodes = self.serializer_nodes_for_model(model=self.models[model], use_memory_writer=False)
				values = instances.values()
				source = g.add_chain(GraphListSource(values))
				self.add_serialization_chain(g, source.output, model=self.models[model], use_memory_writer=False)
				self.run_graph(g, services={})

	def generate_prev_post_sales_data(self, post_map):
		total = 0
		mapped = 0

		g = self.load_sales_tree()
		for src, dst in post_map.items():
			total += 1
			mapped += 1
			g.add_edge(src, dst)
# 		print(f'mapped {mapped}/{total} objects to a previous sale', file=sys.stderr)

		large_components = set(g.largest_component_canonical_keys(10))
		dot = graphviz.Digraph()

		node_id = lambda n: f'n{n!s}'
		for n, i in g.nodes.items():
			key, _ = g.canonical_key(n)
			if key in large_components:
				dot.node(node_id(i), str(n))

		post_sale_rewrite_map = self.load_prev_post_sales_data()
# 		print('Rewrite output files, replacing the following URIs:')
		for src, dst in g:
			canonical, steps = g.canonical_key(src)
			src_uri = self.helper.make_proj_uri('OBJ', *src)
			dst_uri = self.helper.make_proj_uri('OBJ', *canonical)
# 			print(f's/ {src_uri:<100} / {dst_uri:<100} /')
			post_sale_rewrite_map[src_uri] = dst_uri
			if canonical in large_components:
				i = node_id(g.nodes[src])
				j = node_id(g.nodes[dst])
				dot.edge(i, j, f'{steps} steps')

		self.persist_prev_post_sales_data(post_sale_rewrite_map)

		dot_filename = os.path.join(settings.pipeline_tmp_path, 'sales.dot')
		dot.save(filename=dot_filename)
		self.persist_sales_tree(g)

class SalesFilePipeline(SalesPipeline):
	'''
	Sales pipeline with serialization to files based on Arches model and resource UUID.

	If in `debug` mode, JSON serialization will use pretty-printing. Otherwise,
	serialization will be compact.
	'''
	def __init__(self, input_path, catalogs, auction_events, contents, **kwargs):
		super().__init__(input_path, catalogs, auction_events, contents, **kwargs)
		self.writers = []
		self.output_path = kwargs.get('output_path')

	def serializer_nodes_for_model(self, *args, model=None, use_memory_writer=True, **kwargs):
		nodes = []
		kwargs['compact'] = not self.debug
		if use_memory_writer:
			w = MergingMemoryWriter(directory=self.output_path, partition_directories=True, model=model, **kwargs)
		else:
			w = MergingFileWriter(directory=self.output_path, partition_directories=True, model=model, **kwargs)
		nodes.append(w)
		self.writers += nodes
		return nodes

	@staticmethod
	def persist_sales_tree(g):
		sales_tree_filename = os.path.join(settings.pipeline_tmp_path, 'sales-tree.data')
		with open(sales_tree_filename, 'w') as f:
			g.dump(f)

	@staticmethod
	def load_sales_tree():
		sales_tree_filename = os.path.join(settings.pipeline_tmp_path, 'sales-tree.data')
		if os.path.exists(sales_tree_filename):
			with open(sales_tree_filename) as f:
				g = SalesTree.load(f)
		else:
			g = SalesTree()
		return g

	@staticmethod
	def load_prev_post_sales_data():
		rewrite_map_filename = os.path.join(settings.pipeline_tmp_path, 'post_sale_rewrite_map.json')
		post_sale_rewrite_map = {}
		if os.path.exists(rewrite_map_filename):
			with open(rewrite_map_filename, 'r') as f:
				with suppress(json.decoder.JSONDecodeError):
					post_sale_rewrite_map = json.load(f)
		return post_sale_rewrite_map

	@staticmethod
	def persist_prev_post_sales_data(post_sale_rewrite_map):
		rewrite_map_filename = os.path.join(settings.pipeline_tmp_path, 'post_sale_rewrite_map.json')
		print(rewrite_map_filename)
		with open(rewrite_map_filename, 'w') as f:
			json.dump(post_sale_rewrite_map, f)
			print(f'Saved post-sales rewrite map to {rewrite_map_filename}')

	def checkpoint(self):
		self.flush_writers(verbose=False)
		super().checkpoint()

	def flush_writers(self, **kwargs):
		verbose = kwargs.get('verbose', True)
		count = len(self.writers)
		for seq_no, w in enumerate(self.writers):
			if verbose:
				print('[%d/%d] writers being flushed' % (seq_no+1, count))
			if isinstance(w, MergingMemoryWriter):
				w.flush(**kwargs)

	def run(self, **options):
		'''Run the Sales bonobo pipeline.'''
		start = timeit.default_timer()
		services = self.get_services(**options)
		super().run(services=services, **options)
		print(f'Pipeline runtime: {timeit.default_timer() - start}', file=sys.stderr)

		self.flush_writers()

		print('====================================================')
		print('Compiling post-sale data...')
		post_map = services['post_sale_map']
		self.generate_prev_post_sales_data(post_map)
		print(f'>>> {len(post_map)} post sales records')

# 		sizes = {k: sys.getsizeof(v) for k, v in services.items()}
# 		for k in sorted(services.keys(), key=lambda k: sizes[k]):
# 			print(f'{k:<20}  {sizes[k]}')
# 		objgraph.show_most_common_types(limit=50)
		
		print('Record counts:')
		for k, v in services['counts'].items():
			print(f'{v:<10} {k}')
		print('\n\n')
		print('Total runtime: ', timeit.default_timer() - start)

# 		for type in ('AttributeAssignment', 'Person', 'Production', 'Painting'):
# 			objects = objgraph.by_type(type)
# 			for i in range(min(5, len(objects))):
# 				objgraph.show_chain(
# 					objgraph.find_backref_chain(
# 						random.choice(objects),
# 						objgraph.is_proper_module
# 					),
# 					filename=f'chain.{type}.{i}.png'
# 				)

