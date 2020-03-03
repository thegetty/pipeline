'''
Classes and utility functions for instantiating, configuring, and
running a bonobo pipeline for converting Provenance Index CSV data into JSON-LD.
'''

# PIR Extracters

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
from pipeline.projects import PipelineBase, UtilityHelper
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

def make_ordinal(n):
	n = int(n)
	suffix = ['th', 'st', 'nd', 'rd', 'th'][min(n % 10, 4)]
	if 11 <= (n % 100) <= 13:
		suffix = 'th'
	return f'{n}{suffix}'

class PersonIdentity:
	'''
	Utility class to help assign records for people with properties such as `uri` and identifiers.
	'''
	def __init__(self, *, make_shared_uri, make_proj_uri):
		self.make_shared_uri = make_shared_uri
		self.make_proj_uri = make_proj_uri
		self.ignore_authnames = CaseFoldingSet(('NEW', 'NON-UNIQUE'))
		self.make_la_person = pipeline.linkedart.MakeLinkedArtPerson()
		self.make_la_org = pipeline.linkedart.MakeLinkedArtOrganization()
		self.anon_dated_re = re.compile(r'\[ANONYMOUS - (\d+)TH C[.]\]')
		self.anon_period_re = re.compile(r'\[ANONYMOUS - (MODERN|ANTIQUE)\]')
		self.anon_dated_nationality_re = re.compile(r'\[(\w+) - (\d+)TH C[.]\]')
		self.anon_nationality_re = re.compile(r'\[(?!ANON)(\w+)\]', re.IGNORECASE)

	def acceptable_person_auth_name(self, auth_name):
		if not auth_name or auth_name in self.ignore_authnames:
			return False
		elif '[' in auth_name:
			return False
		return True

	def is_anonymous_group(self, auth_name):
		if self.anon_nationality_re.match(auth_name):
			return True
		if self.anon_dated_nationality_re.match(auth_name):
			return True
		elif self.anon_dated_re.match(auth_name):
			return True
		elif self.anon_period_re.match(auth_name):
			return True
		return False

	def is_anonymous(self, data:dict):
		auth_name = data.get('auth_name')
		if auth_name:
			if self.is_anonymous_group(auth_name):
				return False
			return '[ANONYMOUS' in auth_name
		elif data.get('name'):
			return False

		with suppress(ValueError, TypeError):
			if int(data.get('ulan')):
				return False
		return True

	def _uri_keys(self, data:dict, record_id=None):
		ulan = None
		with suppress(ValueError, TypeError):
			ulan = int(data.get('ulan'))

		auth_name = data.get('auth_name')
		auth_name_q = '?' in data.get('auth_nameq', '')

		if ulan:
			key = ('PERSON', 'ULAN', ulan)
			return key, self.make_shared_uri
		elif self.is_anonymous_group(auth_name):
			key = ('GROUP', 'AUTH', auth_name)
			return key, self.make_shared_uri
		elif self.acceptable_person_auth_name(auth_name):
			key = ('PERSON', 'AUTH', auth_name)
			return key, self.make_shared_uri
		else:
			# not enough information to identify this person uniquely, so use the source location in the input file
			pi_rec_no = data['pi_record_no']
			if record_id:
				key = ('PERSON', 'PI', pi_rec_no, record_id)
				return key, self.make_proj_uri
			else:
				warnings.warn(f'*** No record identifier given for person identified only by pi_record_number {pi_rec_no}')
				key = ('PERSON', 'PI', pi_rec_no)
				return key, self.make_proj_uri

	def add_person(self, a, sales_record, relative_id, **kwargs):
		self.add_uri(a, record_id=relative_id)
		self.add_names(a, referrer=sales_record, **kwargs)
		self.add_props(a, **kwargs)
		auth_name = a.get('auth_name')
		if auth_name and self.is_anonymous_group(auth_name):
			self.make_la_org(a)
		else:
			self.make_la_person(a)
		return get_crom_object(a)

	def add_uri(self, data:dict, **kwargs):
		keys, make = self._uri_keys(data, **kwargs)
		data['uri_keys'] = keys
		data['uri'] = make(*keys)

	def timespan_for_century(self, century):
		ord = make_ordinal(century)
		ts = model.TimeSpan(ident='', label=f'{ord} century')
		from_year = 100 * (century-1)
		to_year = from_year + 100
		ts.end_of_the_begin = "%04d-%02d-%02dT%02d:%02d:%02dZ" % (from_year, 1, 1, 0, 0, 0)
		ts.begin_of_the_end = "%04d-%02d-%02dT%02d:%02d:%02dZ" % (to_year, 1, 1, 0, 0, 0)
		return ts

	def anonymous_group_label(self, role, century=None, nationality=None):
		if century and nationality:
			ord = make_ordinal(century)
			return f'{nationality.capitalize()} {role}s in the {ord} century'
		elif century:
			ord = make_ordinal(century)
			return f'{role}s in the {ord} century'
		elif nationality:
			return f'{nationality.capitalize()} {role}s'
		else:
			return f'{role}s'
		return a
		
	def professional_activity(self, group_label, century=None):
		a = vocab.Active(ident='', label=f'Professional activity of {group_label}')
		if century:
			ts = self.timespan_for_century(century)
			a.timespan = ts
		return a

	def add_props(self, data:dict, role=None, **kwargs):
		role = role if role else 'person'
		auth_name = data.get('auth_name', '')
		period_match = self.anon_period_re.match(auth_name)
		nationalities = []
		if 'nationality' in data:
			if isinstance(data['nationality'], str):
				nationalities.append(data['nationality'])
			elif isinstance(data['nationality'], list):
				nationalities += data['nationality']
		data['nationality'] = []
		
		if 'referred_to_by' not in data:
			data['referred_to_by'] = []

		if data.get('name_cite'):
			cite = vocab.BibliographyStatement(ident='', content=data['name_cite'])
			data['referred_to_by'].append(cite)

		if self.is_anonymous_group(auth_name):
			nationality_match = self.anon_nationality_re.match(auth_name)
			dated_nationality_match = self.anon_dated_nationality_re.match(auth_name)
			dated_match = self.anon_dated_re.match(auth_name)
			if 'events' not in data:
				data['events'] = []
			if nationality_match:
				with suppress(ValueError):
					nationality = nationality_match.group(1).lower()
					nationalities.append(nationality)
					group_label = self.anonymous_group_label(role, nationality=nationality)
					data['label'] = group_label
			elif dated_nationality_match:
				with suppress(ValueError):
					nationality = dated_nationality_match.group(1).lower()
					nationalities.append(nationality)
					century = int(dated_nationality_match.group(2))
					group_label = self.anonymous_group_label(role, century=century, nationality=nationality)
					data['label'] = group_label
					a = self.professional_activity(group_label, century=century)
					data['events'].append(a)
			elif dated_match:
				with suppress(ValueError):
					century = int(dated_match.group(1))
					group_label = self.anonymous_group_label(role, century=century)
					data['label'] = group_label
					a = self.professional_activity(group_label, century=century)
					data['events'].append(a)
			elif period_match:
				period = period_match.group(1).lower()
				data['label'] = f'anonymous {period} {role}s'
		for nationality in nationalities:
			key = f'{nationality} nationality'
			n = vocab.instances.get(key)
			if n:
				data['nationality'].append(n)
			else:
				warnings.warn(f'No nationality instance found in crom for: {nationality}')

	def add_names(self, data:dict, referrer=None, role=None, **kwargs):
		'''
		Based on the presence of `auth_name` and/or `name` fields in `data`, sets the
		`label`, `names`, and `identifier` keys to appropriate strings/`model.Identifier`
		values.

		If the `role` string is given (e.g. 'artist'), also sets the `role_label` key
		to a value (e.g. 'artist “RUBENS, PETER PAUL”').
		'''
		auth_name = data.get('auth_name', '')
		role_label = None
		if self.acceptable_person_auth_name(auth_name):
			if role:
				role_label = f'{role} “{auth_name}”'
			data['label'] = auth_name
			pname = vocab.PrimaryName(ident='', content=auth_name) # NOTE: most of these are also vocab.SortName, but not 100%, so witholding that assertion for now
			if referrer:
				pname.referred_to_by = referrer
			data['identifiers'] = [pname]

		name = data.get('name')
		if name:
			if role and not role_label:
				role_label = f'{role} “{name}”'
			if referrer:
				data['names'] = [(name, {'referred_to_by': [referrer]})]
			else:
				data['names'] = [name]
			if 'label' not in data:
				data['label'] = name
		if 'label' not in data:
			data['label'] = '(Anonymous)'

		if role and not role_label:
			role_label = f'anonymous {role}'

		if role:
			data['role_label'] = role_label

class ProvenanceUtilityHelper(UtilityHelper):
	'''
	Project-specific code for accessing and interpreting sales data.
	'''
	def __init__(self, project_name):
		super().__init__(project_name)
		# TODO: does this handle all the cases of data packed into the lot_number string that need to be stripped?
		self.shared_lot_number_re = re.compile(r'(\[[a-z]\])')
		self.ignore_house_authnames = CaseFoldingSet(('Anonymous', '[Anonymous]'))
		self.csv_source_columns = ['pi_record_no', 'star_record_no', 'catalog_number']
		self.problematic_record_uri = f'tag:getty.edu,2019:digital:pipeline:{project_name}:ProblematicRecord'
		self.person_identity = PersonIdentity(make_shared_uri=self.make_shared_uri, make_proj_uri=self.make_proj_uri)
		self.uid_tag_prefix = UID_TAG_PREFIX

	def copy_source_information(self, dst: dict, src: dict):
		for k in self.csv_source_columns:
			with suppress(KeyError):
				dst[k] = src[k]
		return dst

	def add_person(self, data, record, relative_id, **kwargs):
		if data.get('name_so'):
			# handling of the name_so field happens here and not in the PersonIdentity methods,
			# because it requires access to the services data on catalogs
			source = data['name_so']
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
		return self.person_identity.add_person(data, record, relative_id, **kwargs)

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
		if sale_type in ('Auction', 'Collection Catalog'):
			catalog = vocab.AuctionCatalogText(ident=uri, label=f'Sale Catalog {cno}')
		elif sale_type == 'Private Contract Sale':
			catalog = vocab.ExhibitionCatalogText(ident=uri, label=f'Private Sale Exhibition Catalog {cno}')
		elif sale_type == 'Stock List':
			catalog = vocab.AccessionCatalogText(ident=uri, label=f'Accession Catalog {cno}')
		elif sale_type == 'Lottery':
			catalog = vocab.LotteryCatalogText(ident=uri, label=f'Lottery Catalog {cno}')
		else:
			catalog = vocab.SalesCatalogText(ident=uri, label=f'Sale Catalog {cno}')
		return catalog

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
	
	def physical_catalog(self, cno, sale_type, owner=None, copy=None):
		uri = self.physical_catalog_uri(cno, owner, copy)
		labels = []
		if owner:
			labels.append(f'owned by “{owner}”')
		if copy:
			labels.append(f'copy {copy}')
		label = ', '.join(labels)
		catalog_type = self.catalog_type_for_sale_type(sale_type)
		if sale_type == 'Auction':
			labels = [f'Sale Catalog {cno}'] + labels
			catalog = catalog_type(ident=uri, label=', '.join(labels))
		elif sale_type == 'Private Contract Sale':
			labels = [f'Private Sale Exhibition Catalog {cno}'] + labels
			catalog = catalog_type(ident=uri, label=', '.join(labels))
		elif sale_type == 'Stock List':
			labels = [f'Stock List {cno}'] + labels
			catalog = catalog_type(ident=uri, label=', '.join(labels))
		elif sale_type == 'Lottery':
			labels = [f'Lottery Catalog {cno}'] + labels
			catalog = catalog_type(ident=uri, label=', '.join(labels))
		else:
			warnings.warn(f'*** Unexpected sale type: {sale_type!r}')
		return catalog

	def sale_for_sale_type(self, sale_type, lot_object_key):
		cno, lno, date = lot_object_key
		uid, uri = self.shared_lot_number_ids(cno, lno, date)
		shared_lot_number = self.shared_lot_number_from_lno(lno)

		lot_type = self.sale_type_for_sale_type(sale_type)
		lot = lot_type(ident=uri)

		if sale_type == 'Auction':
			lot_id = f'{cno} {shared_lot_number} ({date})'
			lot_label = f'Auction of Lot {lot_id}'
			lot._label = lot_label
		elif sale_type in ('Private Contract Sale', 'Stock List'):
			lot_id = f'{cno} {shared_lot_number} ({date})'
			lot_label = f'Sale of {lot_id}'
			lot._label = lot_label
		elif sale_type == 'Lottery':
			lot_id = f'{cno} {shared_lot_number} ({date})'
			lot_label = f'Lottery Drawing for {lot_id}'
			lot._label = lot_label
		else:
			warnings.warn(f'*** Unexpected sale type: {sale_type!r}')
		return lot

	def sale_event_for_catalog_number(self, catalog_number, sale_type='Auction'):
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
		label = f"{sale_type} Event for {catalog_number}"
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
			n = p.get('price_note')
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
			n = p.get('price_note')
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
			n = p.get('price_note')
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

		auction_house_uri_keys = self.auction_house_uri_keys(a, sequence=sequence)
		a['uri'] = self.auction_house_uri(a, sequence=sequence)
		a['uid'] = '-'.join([str(k) for k in auction_house_uri_keys])
		ulan = None
		with suppress(ValueError, TypeError):
			ulan = int(a.get('ulan'))
		auth_name = a.get('auth')
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
		if name:
			n = model.Name(ident='', content=name)
			if event_record:
				n.referred_to_by = event_record
			a['identifiers'].append(n)
			if 'label' not in a:
				a['label'] = name
		else:
			a['label'] = '(Anonymous)'

		make_house = pipeline.linkedart.MakeLinkedArtAuctionHouseOrganization()
		make_house(a)
		house = get_crom_object(a)

		return add_crom_data(data=a, what=house)

def add_crom_price(data, parent, services, add_citations=False):
	'''
	Add modeling data for `MonetaryAmount`, `StartingPrice`, or `EstimatedPrice`,
	based on properties of the supplied `data` dict.
	'''
	currencies = services['currencies']
	region_currencies = services['region_currencies']
	cno = parent['catalog_number']
	region, _ = cno.split('-', 1)
	if region in region_currencies:
		c = currencies.copy()
		c.update(region_currencies[region])
		amnt = extract_monetary_amount(data, currency_mapping=c, add_citations=add_citations)
	else:
		amnt = extract_monetary_amount(data, currency_mapping=currencies, add_citations=add_citations)
	if amnt:
		add_crom_data(data=data, what=amnt)
	return data


#mark - Provenance Pipeline class

class SalesPipeline(PipelineBase):
	'''Bonobo-based pipeline for transforming Provenance data from CSV into JSON-LD.'''
	def __init__(self, input_path, catalogs, auction_events, contents, **kwargs):
		project_name = 'sales'
		self.input_path = input_path
		self.services = None

		helper = ProvenanceUtilityHelper(project_name)
		self.uid_tag_prefix = UID_TAG_PREFIX

		vocab.register_instance('act of selling', {'parent': model.Activity, 'id': 'XXXXXX001', 'label': 'Act of Selling'})
		vocab.register_instance('act of returning', {'parent': model.Activity, 'id': '300438467', 'label': 'Returning'})
		vocab.register_instance('act of completing sale', {'parent': model.Activity, 'id': 'XXXXXX003', 'label': 'Act of Completing Sale'})

		vocab.register_instance('fire', {'parent': model.Type, 'id': '300068986', 'label': 'Fire'})
		vocab.register_instance('animal', {'parent': model.Type, 'id': '300249395', 'label': 'Animal'})
		vocab.register_instance('history', {'parent': model.Type, 'id': '300033898', 'label': 'History'})

		warnings.warn(f'*** TODO: NEED TO USE CORRECT CLASSIFICATION FOR NON-AUCTION SALES ACTIVITIES, LOTTERY CATALOG')
		vocab.register_vocab_class('LotteryDrawing', {"parent": model.Activity, "id":"000000000", "label": "Lottery Drawing"})
		vocab.register_vocab_class('Lottery', {"parent": model.Activity, "id":"000000000", "label": "Lottery"})
		vocab.register_vocab_class('LotteryCatalog', {"parent": model.HumanMadeObject, "id":"000000000", "label": "Lottery Catalog", "metatype": "work type"})
		vocab.register_vocab_class('LotteryCatalogText', {"parent": model.LinguisticObject, "id":"000000000", "label": "Lottery Catalog", "metatype": "work type"})

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
		services.update({
			# to avoid constructing new MakeLinkedArtPerson objects millions of times, this
			# is passed around as a service to the functions and classes that require it.
			'make_la_person': pipeline.linkedart.MakeLinkedArtPerson(),
			'unique_catalogs': defaultdict(set),
			'post_sale_map': {},
			'event_properties': {
				'auction_houses': defaultdict(list),
				'auction_locations': {},
				'experts': defaultdict(list),
				'commissaire': defaultdict(list),
			},
			'non_auctions': {},
		})
		return services

	def add_physical_catalogs_chain(self, graph, records, serialize=True):
		'''Add modeling of physical copies of auction catalogs.'''
		catalogs = graph.add_chain(
			pipeline.projects.sales.catalogs.AddAuctionCatalog(helper=self.helper),
			pipeline.projects.sales.catalogs.AddPhysicalCatalogObjects(helper=self.helper),
			pipeline.projects.sales.catalogs.AddPhysicalCatalogOwners(helper=self.helper),
			_input=records.output
		)
		if serialize:
			# write SALES data
			self.add_serialization_chain(graph, catalogs.output, model=self.models['HumanMadeObject'], use_memory_writer=False)
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
			GroupRepeatingKeys(
				drop_empty=True,
				mapping={
					'seller': {'prefixes': ('sell_auth_name', 'sell_auth_q')},
					'expert': {
						'postprocess': [
							lambda x, _: replace_key_pattern(r'^(expert)$', 'expert_name', x),
							lambda x, _: strip_key_prefix('expert_', x),
							lambda x, _: replace_key_pattern(r'^(auth)$', 'auth_name', x),
						],
						'prefixes': ('expert', 'expert_auth', 'expert_ulan')
					},
					'commissaire': {
						'postprocess': [
							lambda x, _: replace_key_pattern(r'^(comm_pr)$', 'comm_pr_name', x),
							lambda x, _: strip_key_prefix('comm_pr_', x),
							lambda x, _: replace_key_pattern(r'^(auth)$', 'auth_name', x),
						],
						'prefixes': ('comm_pr', 'comm_pr_auth', 'comm_pr_ulan')
					},
					'auction_house': {
						'postprocess': [
							lambda x, _: strip_key_prefix('auc_house_', x),
						],
						'prefixes': ('auc_house_name', 'auc_house_auth', 'auc_house_ulan')
					},
					'portal': {'prefixes': ('portal_url',)},
				}
			),
			GroupKeys(
				drop_empty=True,
				mapping={
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
					'location': {
						'properties': (
							'city_of_sale',
							'sale_location',
							'country_auth',
							'specific_loc')},
				}
			),
			pipeline.projects.sales.catalogs.AddAuctionCatalog(helper=self.helper),
			pipeline.projects.sales.events.AddAuctionEvent(helper=self.helper),
			pipeline.projects.sales.events.AddAuctionHouses(helper=self.helper),
			pipeline.projects.sales.events.PopulateAuctionEvent(helper=self.helper),
			_input=records.output
		)
		if serialize:
			# write SALES data
			self.add_serialization_chain(graph, auction_events.output, model=self.models['Activity'], use_memory_writer=False)
		return auction_events

	def add_procurement_chain(self, graph, acquisitions, serialize=True):
		'''Add modeling of the procurement event of an auction of a lot.'''
		p = graph.add_chain(
			ExtractKeyedValues(key='_procurements'),
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
		bids = graph.add_chain(
			ExtractKeyedValue(key='_bidding'),
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
			self.add_serialization_chain(graph, bids.output, model=self.models['Bidding'], use_memory_writer=False)
			self.add_serialization_chain(graph, drawing.output, model=self.models['Drawing'], use_memory_writer=False)
		return bid_acqs

	def add_sales_chain(self, graph, records, services, serialize=True):
		'''Add transformation of sales records to the bonobo pipeline.'''
		sales = graph.add_chain(
			RemoveKeys(
				# these are fields that are duplicated here in the contents data,
				# but should only be used in the (event) descriptions data.
				# removing them here ensures that they are not mistakenly used.
				keys={
					'expert_auth_1',
					'expert_ulan_1',
					'expert_auth_2',
					'expert_ulan_2',
					'expert_auth_3',
					'expert_ulan_3',
					'expert_auth_4',
					'expert_ulan_4',
					'commissaire_pr_1',
					'comm_ulan_1',
					'commissaire_pr_2',
					'comm_ulan_2',
					'commissaire_pr_3',
					'comm_ulan_3',
					'commissaire_pr_4',
					'comm_ulan_4',
					'auction_house_1',
					'house_ulan_1',
					'auction_house_2',
					'house_ulan_2',
					'auction_house_3',
					'house_ulan_3',
					'auction_house_4',
					'house_ulan_4',
				}
			),
			GroupRepeatingKeys(
				drop_empty=True,
				mapping={
					'expert': {'prefixes': ('expert_auth', 'expert_ulan')},
					'commissaire': {'prefixes': ('commissaire_pr', 'comm_ulan')},
					'auction_house': {
						'postprocess': [
							lambda x, _: replace_key_pattern(r'(auction_house)', 'house_name', x),
							lambda x, _: strip_key_prefix('house_', x),
						],
						'prefixes': ('auction_house', 'house_ulan')
					},
					'_artists': {
						'postprocess': [
							filter_empty_person,
							add_pir_record_ids
						],
						'prefixes': (
							'artist_name',
							'artist_info',
							'art_authority',
							'nationality',
							'attrib_mod',
							'attrib_mod_auth',
							'star_rec_no',
							'artist_ulan')},
					'hand_note': {'prefixes': ('hand_note', 'hand_note_so')},
					'seller': {
						'postprocess': [
							lambda x, _: strip_key_prefix('sell_', x),
							filter_empty_person
						],
						'prefixes': (
							'sell_name',
							'sell_name_so',
							'sell_name_ques',
							'sell_mod',
							'sell_auth_name',
							'sell_auth_nameq',
							'sell_auth_mod',
							'sell_auth_mod_a',
							'sell_ulan')},
					'price': {
						'postprocess': lambda d, p: add_crom_price(d, p, services, add_citations=True),
						'prefixes': (
							'price_amount',
							'price_currency',
							'price_note',
							'price_source',
							'price_citation')},
					'buyer': {
						'postprocess': [
							lambda x, _: strip_key_prefix('buy_', x),
							filter_empty_person
						],
						'prefixes': (
							'buy_name',
							'buy_name_so',
							'buy_name_ques',
							'buy_name_cite',
							'buy_mod',
							'buy_auth_name',
							'buy_auth_nameq',
							'buy_auth_mod',
							'buy_auth_mod_a',
							'buy_ulan')},
					'prev_owner': {
						'postprocess': [
							lambda x, _: replace_key_pattern(r'(prev_owner)', 'prev_own', x),
							lambda x, _: strip_key_prefix('prev_', x),
						],
						'prefixes': (
							'prev_owner',
							'prev_own_ques',
							'prev_own_so',
							'prev_own_auth',
							'prev_own_auth_d',
							'prev_own_auth_l',
							'prev_own_auth_q',
							'prev_own_ulan')},
					'other_titles': {
						'postprocess': [
							lambda x, _: strip_key_prefix('prev_sale_', x),
							lambda x, _: strip_key_prefix('post_sale_', x),
							lambda x, _: replace_key_pattern(r'(ttlx)', 'title', x),
							lambda x, _: replace_key_pattern(r'(ttl)', 'title', x)
						],
						'prefixes': (
							'prev_sale_ttlx',
							'post_sale_ttl')},
					'prev_sale': {
						'postprocess': lambda x, _: strip_key_prefix('prev_sale_', x),
						'prefixes': (
							'prev_sale_year',
							'prev_sale_mo',
							'prev_sale_day',
							'prev_sale_loc',
							'prev_sale_lot',
							'prev_sale_ques',
							'prev_sale_artx',
							'prev_sale_ttlx',
							'prev_sale_note',
							'prev_sale_coll',
							'prev_sale_cat')},
					'post_sale': {
						'postprocess': lambda x, _: strip_key_prefix('post_sale_', x),
						'prefixes': (
							'post_sale_year',
							'post_sale_mo',
							'post_sale_day',
							'post_sale_loc',
							'post_sale_lot',
							'post_sale_q',
							'post_sale_art',
							'post_sale_nte',
							'post_sale_col',
							'post_sale_cat')},
					'post_owner': {
						'postprocess': lambda x, _: strip_key_prefix('post_', x),
						'prefixes': (
							'post_own',
							'post_own_q',
							'post_own_so',
							'post_own_auth',
							'post_own_auth_d',
							'post_own_auth_l',
							'post_own_auth_q',
							'post_own_ulan')},
					'portal': {'prefixes': ('portal_url',)},
				}
			),
			GroupKeys(mapping={
				'present_location': {
					'postprocess': lambda x, _: strip_key_prefix('present_loc_', x),
					'properties': (
						'present_loc_geog',
						'present_loc_inst',
						'present_loc_insq',
						'present_loc_insi',
						'present_loc_acc',
						'present_loc_accq',
						'present_loc_note',
					)
				}
			}),
			GroupKeys(mapping={
				'auction_of_lot': {
					'properties': (
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
						'portal')},
				'estimated_price': {
					'postprocess': lambda d, p: add_crom_price(d, p, services, add_citations=True),
					'properties': (
						'est_price',
						'est_price_curr',
						'est_price_desc',
						'est_price_so')},
				'start_price': {
					'postprocess': lambda d, p: add_crom_price(d, p, services, add_citations=True),
					'properties': (
						'start_price',
						'start_price_curr',
						'start_price_desc',
						'start_price_so')},
				'ask_price': {
					'postprocess': lambda d, p: add_crom_price(d, p, services, add_citations=True),
					'properties': (
						'ask_price',
						'ask_price_curr',
						'ask_price_so')},
			}),
			pipeline.projects.sales.lots.AddAuctionOfLot(helper=self.helper),
			_input=records.output
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
			self.add_serialization_chain(graph, auctions_of_lot.output, model=self.models['AuctionOfLot'])
			self.add_serialization_chain(graph, private_sale_activities.output, model=self.models['Activity'])
			self.add_serialization_chain(graph, lottery_drawings.output, model=self.models['Drawing'])
		return sales

	def add_object_chain(self, graph, sales, serialize=True):
		'''Add modeling of the objects described by sales records.'''
		objects = graph.add_chain(
			ExtractKeyedValue(key='_object'),
			pipeline.projects.sales.objects.add_object_type,
			pipeline.projects.sales.objects.PopulateObject(helper=self.helper),
			pipeline.linkedart.MakeLinkedArtHumanMadeObject(),
			pipeline.projects.sales.objects.AddArtists(helper=self.helper),
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

	def add_places_chain(self, graph, auction_events, key='_locations', serialize=True):
		'''Add extraction and serialization of locations.'''
		places = graph.add_chain(
			ExtractKeyedValues(key=key),
			RecursiveExtractKeyedValue(key='part_of'),
			_input=auction_events.output
		)
		if serialize:
			# write OBJECTS data
			self.add_serialization_chain(graph, places.output, model=self.models['Place'])
		return places

	def add_person_or_group_chain(self, graph, input, key=None, serialize=True):
		'''Add extraction and serialization of people and groups.'''
		if key:
			extracted = graph.add_chain(
				ExtractKeyedValues(key=key),
				_input=input.output
			)
		else:
			extracted = input

		people = graph.add_chain(
			OnlyRecordsOfType(type=model.Person),
			_input=extracted.output
		)
		groups = graph.add_chain(
			OnlyRecordsOfType(type=model.Group),
			_input=extracted.output
		)
		if serialize:
			# write OBJECTS data
			self.add_serialization_chain(graph, people.output, model=self.models['Person'])
			self.add_serialization_chain(graph, groups.output, model=self.models['Group'])
		return people

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
				CurriedCSVReader(fs='fs.data.sales', limit=self.limit),
				AddFieldNames(field_names=self.auction_events_headers)
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
				CurriedCSVReader(fs='fs.data.sales', limit=self.limit),
				AddFieldNames(field_names=self.catalogs_headers),
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
				CurriedCSVReader(fs='fs.data.sales', limit=self.limit),
				AddFieldNames(field_names=self.contents_headers),
			)
			sales = self.add_sales_chain(g, contents_records, services, serialize=True)
			_ = self.add_lot_set_chain(g, sales, serialize=True)
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
		'''Construct the bonobo pipeline to fully transform Provenance data from CSV to JSON-LD.'''
		if not self.graph_1:
			self._construct_graph(**kwargs)
		return self.graph_1

	def get_graph_2(self, **kwargs):
		'''Construct the bonobo pipeline to fully transform Provenance data from CSV to JSON-LD.'''
		if not self.graph_2:
			self._construct_graph(**kwargs)
		return self.graph_2

	def get_graph_3(self, **kwargs):
		'''Construct the bonobo pipeline to fully transform Provenance data from CSV to JSON-LD.'''
		if not self.graph_3:
			self._construct_graph(**kwargs)
		return self.graph_3

	def checkpoint(self):
		pass

	def run(self, services=None, **options):
		'''Run the Provenance bonobo pipeline.'''
		print(f'- Limiting to {self.limit} records per file', file=sys.stderr)
		if not services:
			services = self.get_services(**options)

		print('Running graph component 1...', file=sys.stderr)
		graph1 = self.get_graph_1(**options, services=services)
		self.run_graph(graph1, services=services)

		self.checkpoint()
		
		print('Running graph component 2...', file=sys.stderr)
		graph2 = self.get_graph_2(**options, services=services)
		self.run_graph(graph2, services=services)

		self.checkpoint()

		print('Running graph component 3...', file=sys.stderr)
		graph3 = self.get_graph_3(**options, services=services)
		self.run_graph(graph3, services=services)

		self.checkpoint()

		print('Serializing static instances...', file=sys.stderr)
		for model, instances in self.static_instances.used_instances().items():
			g = bonobo.Graph()
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
	Provenance pipeline with serialization to files based on Arches model and resource UUID.

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
		self.flush_writers()
		super().checkpoint()

	def flush_writers(self):
		count = len(self.writers)
		for seq_no, w in enumerate(self.writers):
			print('[%d/%d] writers being flushed' % (seq_no+1, count))
			if isinstance(w, MergingMemoryWriter):
				w.flush()

	def run(self, **options):
		'''Run the Provenance bonobo pipeline.'''
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
		
		sizes = {k: sys.getsizeof(v) for k, v in services.items()}
		for k in sorted(services.keys(), key=lambda k: sizes[k]):
			print(f'{k:<20}  {sizes[k]}')
		print('Total runtime: ', timeit.default_timer() - start)
