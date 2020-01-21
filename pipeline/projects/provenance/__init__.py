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
from pipeline.projects.provenance.util import *
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
			AddFieldNames, \
			GroupRepeatingKeys, \
			GroupKeys, \
			AddArchesModel, \
			Serializer, \
			OnlyCromModeledRecords, \
			Trace
from pipeline.util.rewriting import rewrite_output_files, JSONValueRewriter
import pipeline.projects.provenance.events
import pipeline.projects.provenance.lots
import pipeline.projects.provenance.objects
import pipeline.projects.provenance.catalogs

#mark - utility functions and classes

class PersonIdentity:
	'''
	Utility class to help assign records for people with properties such as `uri` and identifiers.
	'''
	def __init__(self, *, make_uri):
		self.make_uri = make_uri
		self.ignore_authnames = CaseFoldingSet(('NEW', 'NON-UNIQUE'))

	def acceptable_auth_name(self, auth_name):
		if not auth_name or auth_name in self.ignore_authnames:
			return False
		if '[' in auth_name:
			return False
		return True

	@staticmethod
	def is_anonymous(data:dict):
		auth_name = data.get('auth_name')
		if auth_name:
			return '[ANONYMOUS' in auth_name
		elif data.get('name'):
			return False

		with suppress(ValueError, TypeError):
			if int(data.get('ulan')):
				return False
		return True

	def uri_keys(self, data:dict, record_id=None):
		ulan = None
		with suppress(ValueError, TypeError):
			ulan = int(data.get('ulan'))

		auth_name = data.get('auth_name')
		auth_name_q = '?' in data.get('auth_nameq', '')

		if ulan:
			return ('PERSON', 'ULAN', ulan)
		elif self.acceptable_auth_name(auth_name):
			return ('PERSON', 'AUTHNAME', auth_name)
		else:
			# not enough information to identify this person uniquely, so use the source location in the input file
			pi_rec_no = data['pi_record_no']
			if record_id:
				return ('PERSON', 'PI_REC_NO', pi_rec_no, record_id)
			else:
				warnings.warn(f'*** No record identifier given for person identified only by pi_record_number {pi_rec_no}')
				return ('PERSON', 'PI_REC_NO', pi_rec_no)

	def add_uri(self, data:dict, **kwargs):
		keys = self.uri_keys(data, **kwargs)
		data['uri_keys'] = keys
		data['uri'] = self.make_uri(*keys)

	def add_names(self, data:dict, referrer=None, role=None):
		'''
		Based on the presence of `auth_name` and/or `name` fields in `data`, sets the
		`label`, `names`, and `identifier` keys to appropriate strings/`model.Identifier`
		values.

		If the `role` string is given (e.g. 'artist'), also sets the `role_label` key
		to a value (e.g. 'artist “RUBENS, PETER PAUL”').
		'''
		auth_name = data.get('auth_name')
		role_label = None
		if self.acceptable_auth_name(auth_name):
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
		self.ignore_house_authnames = CaseFoldingSet(('Anonymous',))
		self.csv_source_columns = ['pi_record_no', 'catalog_number']
		self.problematic_record_uri = 'tag:getty.edu,2019:digital:pipeline:provenance:ProblematicRecord'
		self.person_identity = PersonIdentity(make_uri=self.make_proj_uri)
		self.uid_tag_prefix = UID_TAG_PREFIX

	def copy_source_information(self, dst: dict, src: dict):
		for k in self.csv_source_columns:
			with suppress(KeyError):
				dst[k] = src[k]
		return dst

	def auction_event_for_catalog_number(self, catalog_number):
		'''
		Return a `vocab.AuctionEvent` object and its associated 'uid' key and URI, based on
		the supplied `catalog_number`.
		'''
		uid = f'AUCTION-EVENT-CATALOGNUMBER-{catalog_number}'
		uri = self.make_proj_uri('AUCTION-EVENT', 'CATALOGNUMBER', catalog_number)
		label = f"Auction Event for {catalog_number}"
		auction = vocab.AuctionEvent(ident=uri, label=label)
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

	def transaction_uri_for_lot(self, data, prices):
		'''
		Return a URI representing the procurement which the object (identified by the
		supplied data) is a part of. This may identify just the lot being sold or, in the
		case of multiple lots being bought for a single price, a single procurement that
		encompasses multiple acquisitions that span different lots.
		'''
		cno, lno, date = object_key(data)
		shared_lot_number = self.shared_lot_number_from_lno(lno)
		for p in prices:
			n = p.get('price_note')
			if n and n.startswith('for lots '):
				return self.make_proj_uri('AUCTION-TX-MULTI', cno, date, n[9:])
		return self.make_proj_uri('AUCTION-TX', cno, date, shared_lot_number)

	def lots_in_transaction(self, data, prices):
		'''
		Return a string that represents the lot numbers that are a part of the procurement
		related to the supplied data.
		'''
		_, lno, _ = object_key(data)
		shared_lot_number = self.shared_lot_number_from_lno(lno)
		for p in prices:
			n = p.get('price_note')
			if n and n.startswith('for lots '):
				return n[9:]
		return shared_lot_number

	def shared_lot_number_ids(self, cno, lno, date):
		'''
		Return a tuple of a UID string and a URI for the lot identified by the supplied
		data which identifies a specific object in that lot.
		'''
		shared_lot_number = self.shared_lot_number_from_lno(lno)
		uid = f'AUCTION-{cno}-LOT-{shared_lot_number}-DATE-{date}'
		uri = self.make_proj_uri('AUCTION', cno, 'LOT', shared_lot_number, 'DATE', date)
		return uid, uri

	@staticmethod
	def transaction_contains_multiple_lots(data, prices):
		'''
		Return `True` if the procurement related to the supplied data represents a
		transaction of multiple lots with a single payment, `False` otherwise.
		'''
		for p in prices:
			n = p.get('price_note')
			if n and n.startswith('for lots '):
				return True
		return False

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


#mark - Single Object Lot Tracking

class TrackLotSizes(Configurable):
	helper = Option(required=True)
	lot_counter = Service('lot_counter')

	def __call__(self, data, lot_counter):
		auction_data = data['auction_of_lot']
		cno, lno, date = object_key(auction_data)
		lot = self.helper.shared_lot_number_from_lno(lno)
		key = (cno, lot, date)
		lot_counter[key] += 1

#mark - Provenance Pipeline class

class ProvenancePipeline(PipelineBase):
	'''Bonobo-based pipeline for transforming Provenance data from CSV into JSON-LD.'''
	def __init__(self, input_path, catalogs, auction_events, contents, **kwargs):
		project_name = 'provenance'
		helper = ProvenanceUtilityHelper(project_name)
		self.uid_tag_prefix = UID_TAG_PREFIX

		vocab.register_instance('fire', {'parent': model.Type, 'id': '300068986', 'label': 'Fire'})
		vocab.register_instance('animal', {'parent': model.Type, 'id': '300249395', 'label': 'Animal'})
		vocab.register_instance('history', {'parent': model.Type, 'id': '300033898', 'label': 'History'})
		vocab.register_vocab_class('AuctionCatalog', {'parent': model.HumanMadeObject, 'id': '300026068', 'label': 'Auction Catalog', 'metatype': 'work type'})

		super().__init__(project_name, helper=helper)

		self.graph_0 = None
		self.graph_1 = None
		self.graph_2 = None
		self.models = kwargs.get('models', settings.arches_models)
		self.catalogs_header_file = catalogs['header_file']
		self.catalogs_files_pattern = catalogs['files_pattern']
		self.auction_events_header_file = auction_events['header_file']
		self.auction_events_files_pattern = auction_events['files_pattern']
		self.contents_header_file = contents['header_file']
		self.contents_files_pattern = contents['files_pattern']
		self.limit = kwargs.get('limit')
		self.debug = kwargs.get('debug', False)
		self.input_path = input_path

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

	# Set up environment
	def get_services(self):
		'''Return a `dict` of named services available to the bonobo pipeline.'''
		services = super().get_services()
		services.update({
			# to avoid constructing new MakeLinkedArtPerson objects millions of times, this
			# is passed around as a service to the functions and classes that require it.
			'make_la_person': pipeline.linkedart.MakeLinkedArtPerson(),
			'lot_counter': Counter(),
			'unique_catalogs': {},
			'post_sale_map': {},
			'auction_houses': {},
			'non_auctions': {},
			'auction_locations': {},
		})
		return services

	def add_physical_catalog_owners_chain(self, graph, catalogs, serialize=True):
		'''Add modeling of physical copies of auction catalogs.'''
		groups = graph.add_chain(
			ExtractKeyedValue(key='_owner'),
			pipeline.linkedart.MakeLinkedArtOrganization(),
			_input=catalogs.output
		)
		if serialize:
			# write SALES data
			self.add_serialization_chain(graph, groups.output, model=self.models['Group'])
		return groups

	def add_physical_catalogs_chain(self, graph, records, serialize=True):
		'''Add modeling of physical copies of auction catalogs.'''
		catalogs = graph.add_chain(
			pipeline.projects.provenance.catalogs.AddAuctionCatalog(helper=self.helper),
			pipeline.projects.provenance.catalogs.AddPhysicalCatalogObjects(helper=self.helper),
			pipeline.projects.provenance.catalogs.AddPhysicalCatalogOwners(helper=self.helper),
			_input=records.output
		)
		if serialize:
			# write SALES data
			self.add_serialization_chain(graph, catalogs.output, model=self.models['HumanMadeObject'])
		return catalogs

	def add_catalog_linguistic_objects_chain(self, graph, events, serialize=True):
		'''Add modeling of auction catalogs as linguistic objects.'''
		los = graph.add_chain(
			ExtractKeyedValue(key='_catalog'),
			pipeline.projects.provenance.catalogs.PopulateAuctionCatalog(static_instances=self.static_instances),
			_input=events.output
		)
		if serialize:
			# write SALES data
			self.add_serialization_chain(graph, los.output, model=self.models['LinguisticObject'])
		return los

	def add_auction_events_chain(self, graph, records, serialize=True):
		'''Add modeling of auction events.'''
		auction_events = graph.add_chain(
			GroupRepeatingKeys(
				drop_empty=True,
				mapping={
					'seller': {'prefixes': ('sell_auth_name', 'sell_auth_q')},
					'expert': {'prefixes': ('expert', 'expert_auth', 'expert_ulan')},
					'commissaire': {'prefixes': ('comm_pr', 'comm_pr_auth', 'comm_pr_ulan')},
					'auction_house': {'prefixes': ('auc_house_name', 'auc_house_auth', 'auc_house_ulan')},
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
			pipeline.projects.provenance.catalogs.AddAuctionCatalog(helper=self.helper),
			pipeline.projects.provenance.events.AddAuctionEvent(helper=self.helper),
			pipeline.projects.provenance.events.AddAuctionHouses(helper=self.helper),
			pipeline.projects.provenance.events.PopulateAuctionEvent(helper=self.helper),
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
		buyers = graph.add_chain(
			ExtractKeyedValues(key='buyer'),
			_input=acquisitions.output
		)

		owners = graph.add_chain(
			ExtractKeyedValues(key='_other_owners'),
			_input=acquisitions.output
		)

		sellers = graph.add_chain(
			ExtractKeyedValues(key='seller'),
			_input=acquisitions.output
		)

		if serialize:
			# write SALES data
			self.add_serialization_chain(graph, owners.output, model=self.models['Person'])
			self.add_serialization_chain(graph, buyers.output, model=self.models['Person'])
			self.add_serialization_chain(graph, sellers.output, model=self.models['Person'])

	def add_acquisitions_chain(self, graph, sales, serialize=True):
		'''Add modeling of the acquisitions and bidding on lots being auctioned.'''
		bid_acqs = graph.add_chain(
			pipeline.projects.provenance.lots.AddAcquisitionOrBidding(helper=self.helper),
			_input=sales.output
		)
		orgs = graph.add_chain(
			ExtractKeyedValues(key='_organizations'),
			_input=bid_acqs.output
		)
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
		places = graph.add_chain(
			ExtractKeyedValues(key='_owner_locations'),
			_input=bid_acqs.output
		)

		if serialize:
			# write SALES data
			self.add_serialization_chain(graph, refs.output, model=self.models['LinguisticObject'])
			self.add_serialization_chain(graph, bids.output, model=self.models['Bidding'])
			self.add_serialization_chain(graph, orgs.output, model=self.models['Group'])
			self.add_serialization_chain(graph, places.output, model=self.models['Place'])
		return bid_acqs

	def add_sales_chain(self, graph, records, services, serialize=True):
		'''Add transformation of sales records to the bonobo pipeline.'''
		sales = graph.add_chain(
			GroupRepeatingKeys(
				drop_empty=True,
				mapping={
					'expert': {'prefixes': ('expert_auth', 'expert_ulan')},
					'commissaire': {'prefixes': ('commissaire_pr', 'comm_ulan')},
					'auction_house': {'prefixes': ('auction_house', 'house_ulan')},
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
							'post_sale_ttl',
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
			pipeline.projects.provenance.lots.AddAuctionOfLot(helper=self.helper),
			_input=records.output
		)
		
		modeled_sales = graph.add_chain(
			OnlyCromModeledRecords(),
			_input=sales.output
		)
		
		if serialize:
			# write SALES data
			self.add_serialization_chain(graph, modeled_sales.output, model=self.models['AuctionOfLot'])
		return sales

	def add_single_object_lot_tracking_chain(self, graph, sales):
		small_lots = graph.add_chain(
			TrackLotSizes(helper=self.helper),
			_input=sales.output
		)
		return small_lots

	def add_object_chain(self, graph, sales, serialize=True):
		'''Add modeling of the objects described by sales records.'''
		objects = graph.add_chain(
			ExtractKeyedValue(key='_object'),
			pipeline.projects.provenance.objects.add_object_type,
			pipeline.projects.provenance.objects.PopulateObject(helper=self.helper),
			pipeline.linkedart.MakeLinkedArtHumanMadeObject(),
			pipeline.projects.provenance.objects.AddArtists(helper=self.helper),
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
			self.add_serialization_chain(graph, objects.output, model=self.models['HumanMadeObject'])
			self.add_serialization_chain(graph, original_objects.output, model=self.models['HumanMadeObject'])

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
			pipeline.linkedart.MakeLinkedArtAuctionHouseOrganization(),
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

	def _construct_graph(self, single_graph=False, services=None):
		'''
		Construct bonobo.Graph object(s) for the entire pipeline.

		If `single_graph` is `False`, generate two `Graph`s (`self.graph_1` and
		`self.graph_2`), that will be run sequentially. the first for catalogs and events,
		the second for sales auctions (which depends on output from the first).

		If `single_graph` is `True`, then generate a single `Graph` that has the entire
		pipeline in it (`self.graph_0`). This is used to be able to produce graphviz
		output of the pipeline for visual inspection.
		'''
		graph0 = bonobo.Graph()
		graph1 = bonobo.Graph()
		graph2 = bonobo.Graph()

		component1 = [graph0] if single_graph else [graph1]
		component2 = [graph0] if single_graph else [graph2]
		for g in component1:
			physical_catalog_records = g.add_chain(
				MatchingFiles(path='/', pattern=self.catalogs_files_pattern, fs='fs.data.provenance'),
				CurriedCSVReader(fs='fs.data.provenance', limit=self.limit),
				AddFieldNames(field_names=self.catalogs_headers),
			)

			auction_events_records = g.add_chain(
				MatchingFiles(path='/', pattern=self.auction_events_files_pattern, fs='fs.data.provenance'),
				CurriedCSVReader(fs='fs.data.provenance', limit=self.limit),
				AddFieldNames(field_names=self.auction_events_headers),
			)

			catalogs = self.add_physical_catalogs_chain(g, physical_catalog_records, serialize=True)
			_ = self.add_physical_catalog_owners_chain(g, catalogs, serialize=True)
			auction_events = self.add_auction_events_chain(g, auction_events_records, serialize=True)
			_ = self.add_catalog_linguistic_objects_chain(g, auction_events, serialize=True)
			_ = self.add_auction_houses_chain(g, auction_events, serialize=True)
			_ = self.add_places_chain(g, auction_events, serialize=True)

		for g in component2:
			contents_records = g.add_chain(
				MatchingFiles(path='/', pattern=self.contents_files_pattern, fs='fs.data.provenance'),
				CurriedCSVReader(fs='fs.data.provenance', limit=self.limit),
				AddFieldNames(field_names=self.contents_headers),
			)
			sales = self.add_sales_chain(g, contents_records, services, serialize=True)
			_ = self.add_single_object_lot_tracking_chain(g, sales)
			_ = self.add_lot_set_chain(g, sales, serialize=True)
			objects = self.add_object_chain(g, sales, serialize=True)
			_ = self.add_places_chain(g, objects, serialize=True)
			acquisitions = self.add_acquisitions_chain(g, objects, serialize=True)
			self.add_buyers_sellers_chain(g, acquisitions, serialize=True)
			self.add_procurement_chain(g, acquisitions, serialize=True)
			_ = self.add_people_chain(g, objects, serialize=True)
			_ = self.add_record_text_chain(g, objects, serialize=True)
			_ = self.add_visual_item_chain(g, objects, serialize=True)

		if single_graph:
			self.graph_0 = graph0
		else:
			self.graph_1 = graph1
			self.graph_2 = graph2

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

	def run(self, services=None, **options):
		'''Run the Provenance bonobo pipeline.'''
		print(f'- Limiting to {self.limit} records per file', file=sys.stderr)
		if not services:
			services = self.get_services(**options)

		print('Running graph component 1...', file=sys.stderr)
		graph1 = self.get_graph_1(**options, services=services)
		self.run_graph(graph1, services=services)

		print('Running graph component 2...', file=sys.stderr)
		graph2 = self.get_graph_2(**options, services=services)
		self.run_graph(graph2, services=services)

		print('Serializing static instances...', file=sys.stderr)
		for model, instances in self.static_instances.used_instances().items():
			g = bonobo.Graph()
			nodes = self.serializer_nodes_for_model(model=self.models[model], use_memory_writer=False)
			values = instances.values()
			source = g.add_chain(GraphListSource(values))
			self.add_serialization_chain(g, source.output, model=self.models[model], use_memory_writer=False)
			self.run_graph(g, services={})

	def generate_prev_post_sales_data(self, counter, post_map):
		singles = {k for k in counter if counter[k] == 1}
		multiples = {k for k in counter if counter[k] > 1}

		total = 0
		mapped = 0

		g = self.load_sales_tree()
		for src, dst in post_map.items():
			total += 1
			if dst in singles:
				mapped += 1
				g.add_edge(src, dst)
# 			elif dst in multiples:
# 				print(f'  {src} maps to a MULTI-OBJECT lot')
# 			else:
# 				print(f'  {src} maps to an UNKNOWN lot')
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
			src_uri = self.helper.make_proj_uri('OBJECT', *src)
			dst_uri = self.helper.make_proj_uri('OBJECT', *canonical)
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

class ProvenanceFilePipeline(ProvenancePipeline):
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
		with open(rewrite_map_filename, 'w') as f:
			json.dump(post_sale_rewrite_map, f)
			print(f'Saved post-sales rewrite map to {rewrite_map_filename}')

	def run(self, **options):
		'''Run the Provenance bonobo pipeline.'''
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
		print('Running post-processing of post-sale data...')
		counter = services['lot_counter']
		post_map = services['post_sale_map']
		self.generate_prev_post_sales_data(counter, post_map)
		print(f'>>> {len(post_map)} post sales records')
		print('Total runtime: ', timeit.default_timer() - start)
