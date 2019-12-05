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
from pipeline.projects import PipelineBase
from pipeline.projects.provenance.util import *
from pipeline.util import \
			truncate_with_ellipsis, \
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
import pipeline.linkedart
from pipeline.linkedart import add_crom_data, get_crom_object
from pipeline.io.csv import CurriedCSVReader
from pipeline.nodes.basic import \
			AddFieldNames, \
			GroupRepeatingKeys, \
			GroupKeys, \
			AddArchesModel, \
			Serializer, \
			Trace
from pipeline.util.rewriting import rewrite_output_files, JSONValueRewriter

PROBLEMATIC_RECORD_URI = 'tag:getty.edu,2019:digital:pipeline:provenance:ProblematicRecord'
CSV_SOURCE_COLUMNS = ['pi_record_no', 'catalog_number']

IGNORE_HOUSE_AUTHNAMES = CaseFoldingSet(('Anonymous',))

#mark - utility functions and classes

class PersonIdentity:
	'''
	Utility class to help assign records for people with properties such as `uri` and identifiers.
	'''
	def __init__(self, make_uri=None):
		self.make_uri = make_uri or pir_uri
		self.ignore_authnames = CaseFoldingSet(('NEW', 'NON-UNIQUE'))
	
	def acceptable_auth_name(self, auth_name):
		if not auth_name or auth_name in self.ignore_authnames:
			return False
		if '[' in auth_name:
			return False
		return True

	def uri_keys(self, data:dict, record_id=None):
		ulan = None
		with suppress(ValueError, TypeError):
			ulan = int(data.get('ulan'))

		auth_name = data.get('auth_name')
		auth_name_q = '?' in data.get('auth_nameq', '')
	
		if ulan:
			key = f'PERSON-ULAN-{ulan}'
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

class GraphListSource:
	'''
	Act as a bonobo graph source node for a set of crom objects.
	Yields the supplied objects wrapped in data dicts.
	'''
	def __init__(self, values, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.values = values

	def __call__(self):
		for v in self.values:
			yield add_crom_data({}, v)

def copy_source_information(dst: dict, src: dict):
	for k in CSV_SOURCE_COLUMNS:
		with suppress(KeyError):
			dst[k] = src[k]
	return dst

def auction_event_for_catalog_number(catalog_number):
	'''
	Return a `vocab.AuctionEvent` object and its associated 'uid' key and URI, based on
	the supplied `catalog_number`.
	'''
	uid = f'AUCTION-EVENT-CATALOGNUMBER-{catalog_number}'
	uri = pir_uri('AUCTION-EVENT', 'CATALOGNUMBER', catalog_number)
	auction = vocab.AuctionEvent(ident=uri)
	auction._label = f"Auction Event for {catalog_number}"
	return auction, uid, uri

def add_auction_event(data):
	'''Add modeling for an auction event based on properties of the supplied `data` dict.'''
	cno = data['catalog_number']
	auction, uid, uri = auction_event_for_catalog_number(cno)
	data['uid'] = uid
	data['uri'] = uri
	add_crom_data(data=data, what=auction)
	catalog = get_crom_object(data['_catalog'])

	record_uri = pir_uri('AUCTION-EVENT', 'CATALOGNUMBER', cno, 'RECORD')
	label = f'Record of auction event from catalog {cno}'
	record = model.LinguisticObject(ident=record_uri, label=label) # TODO: needs classification
	record_data	= {'uri': record_uri}
	record_data['identifiers'] = [model.Name(ident='', content=label)]
	record.part_of = catalog

	data['_record'] = add_crom_data(data=record_data, what=record)
	return data

#mark - Places

def auction_event_location(data):
	'''
	Based on location data in the supplied `data` dict, construct a data structure
	representing a hierarchy of places (e.g. location->city->country), and return it.

	This structure will be suitable for passing to `pipeline.linkedart.make_la_place`
	to construct a Place model object.
	'''
	specific_name = data.get('specific_loc')
	city_name = data.get('city_of_sale')
	country_name = data.get('country_auth')

	parts = [v for v in (specific_name, city_name, country_name) if v is not None]
	loc = parse_location(*parts, uri_base=UID_TAG_PREFIX, types=('Place', 'City', 'Country'))
	return loc

#mark - Auction Events

@use('auction_locations')
def populate_auction_event(data, auction_locations):
	'''Add modeling data for an auction event'''
	cno = data['catalog_number']
	auction = get_crom_object(data)
	catalog = data['_catalog']['_LOD_OBJECT']

	location_data = data['location']
	current = auction_event_location(location_data)

	# make_la_place is called here instead of as a separate graph node because the Place object
	# gets stored in the `auction_locations` object to be used in the second graph component
	# which uses the data to associate the place with auction lots.
	base_uri = pir_uri('AUCTION-EVENT', 'CATALOGNUMBER', cno, 'PLACE')
	place_data = pipeline.linkedart.make_la_place(current, base_uri=base_uri)
	place = get_crom_object(place_data)
	if place:
		data['_locations'] = [place_data]
		auction.took_place_at = place
		auction_locations[cno] = place

	begin = implode_date(data, 'sale_begin_', clamp='begin')
	end = implode_date(data, 'sale_end_', clamp='eoe')
	ts = timespan_from_outer_bounds(
		begin=begin,
		end=end,
	)
	if begin and end:
		ts.identified_by = model.Name(ident='', content=f'{begin} to {end}')
	elif begin:
		ts.identified_by = model.Name(ident='', content=f'{begin} onwards')
	elif end:
		ts.identified_by = model.Name(ident='', content=f'up to {end}')

	for p in data.get('portal', []):
		url = p['portal_url']
		if url.startswith('http'):
			auction.referred_to_by = vocab.WebPage(ident=url)
		else:
			warnings.warn(f'*** Portal URL value does not appear to be a valid URL: {url}')

	if ts:
		auction.timespan = ts

	auction.subject_of = catalog
	return data

def add_auction_house_data(a, event_record):
	'''Add modeling data for an auction house organization.'''
	catalog = a.get('_catalog')

	ulan = None
	with suppress(ValueError, TypeError):
		ulan = int(a.get('auc_house_ulan'))
	auth_name = a.get('auc_house_auth')
	a['identifiers'] = []
	if ulan:
		key = f'AUCTION-HOUSE-ULAN-{ulan}'
		a['uid'] = key
		a['uri'] = pir_uri('AUCTION-HOUSE', 'ULAN', ulan)
		a['ulan'] = ulan
		house = vocab.AuctionHouseOrg(ident=a['uri'])
	elif auth_name and auth_name not in IGNORE_HOUSE_AUTHNAMES:
		a['uri'] = pir_uri('AUCTION-HOUSE', 'AUTHNAME', auth_name)
		pname = vocab.PrimaryName(ident='', content=auth_name)
		pname.referred_to_by = event_record
		a['identifiers'].append(pname)
		house = vocab.AuctionHouseOrg(ident=a['uri'])
	else:
		# not enough information to identify this house uniquely, so use the source location in the input file
		a['uri'] = pir_uri('AUCTION-HOUSE', 'CAT_NO', 'CATALOG-NUMBER', a['catalog_number'])
		house = vocab.AuctionHouseOrg(ident=a['uri'])

	name = a.get('auc_house_name') or a.get('name')
	if name:
		n = model.Name(ident='', content=name)
		n.referred_to_by = event_record
		a['identifiers'].append(n)
		a['label'] = name
	else:
		a['label'] = '(Anonymous)'

	add_crom_data(data=a, what=house)
	return a

@use('auction_houses')
def add_auction_houses(data, auction_houses):
	'''
	Add modeling data for the auction house organization(s) associated with an auction
	event.
	'''
	auction = get_crom_object(data)
	event_record = get_crom_object(data['_record'])
	catalog = data['_catalog']['_LOD_OBJECT']
	d = data.copy()
	houses = data.get('auction_house', [])
	cno = data['catalog_number']

	house_objects = []
	event_record = get_crom_object(data['_record'])
	for h in houses:
		h['_catalog'] = catalog
		add_auction_house_data(copy_source_information(h, data), event_record)
		house = get_crom_object(h)
		auction.carried_out_by = house
		if auction_houses:
			house_objects.append(house)
	auction_houses[cno] = house_objects
	return d


#mark - Auction of Lot

class AddAuctionOfLot(Configurable):
	'''Add modeling data for the auction of a lot of objects.'''

	# TODO: does this handle all the cases of data packed into the lot_number string that need to be stripped?
	shared_lot_number_re = re.compile(r'(\[[a-z]\])')

	problematic_records = Service('problematic_records')
	auction_locations = Service('auction_locations')
	auction_houses = Service('auction_houses')
	non_auctions = Service('non_auctions')
	def __init__(self, *args, **kwargs):
		self.lot_cache = {}
		super().__init__(*args, **kwargs)

	@staticmethod
	def shared_lot_number_from_lno(lno):
		'''
		Given a `lot_number` value which identifies an object in a group, strip out the
		object-specific content, returning an identifier for the entire lot.

		For example, strip the object identifier suffixes such as '[a]':

		'0001[a]' -> '0001'
		'''
		m = AddAuctionOfLot.shared_lot_number_re.search(lno)
		if m:
			return lno.replace(m.group(1), '')
		return lno

	@staticmethod
	def set_lot_auction_houses(lot, cno, auction_houses):
		'''Associate the auction house with the auction lot.'''
		houses = auction_houses.get(cno)
		if houses:
			for house in houses:
				lot.carried_out_by = house

	@staticmethod
	def set_lot_location(lot, cno, auction_locations):
		'''Associate the location with the auction lot.'''
		place = auction_locations.get(cno)
		if place:
			lot.took_place_at = place

	@staticmethod
	def set_lot_date(lot, auction_data):
		'''Associate a timespan with the auction lot.'''
		date = implode_date(auction_data, 'lot_sale_')
# 		dates = date_parse(date, delim='-')
# 		if dates:
		if date:
			begin = implode_date(auction_data, 'lot_sale_', clamp='begin')
			end = implode_date(auction_data, 'lot_sale_', clamp='eoe')
			bounds = [begin, end]
		else:
			bounds = []
		if bounds:
			ts = timespan_from_outer_bounds(*bounds)
			ts.identified_by = model.Name(ident='', content=date)
			lot.timespan = ts

	@staticmethod
	def shared_lot_number_ids(cno, lno, date):
		'''
		Return a tuple of a UID string and a URI for the lot identified by the supplied
		data which identifies a specific object in that lot.
		'''
		shared_lot_number = AddAuctionOfLot.shared_lot_number_from_lno(lno)
		uid = f'AUCTION-{cno}-LOT-{shared_lot_number}-DATE-{date}'
		uri = pir_uri('AUCTION', cno, 'LOT', shared_lot_number, 'DATE', date)
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

	@staticmethod
	def lots_in_transaction(data, prices):
		'''
		Return a string that represents the lot numbers that are a part of the procurement
		related to the supplied data.
		'''
		_, lno, _ = object_key(data)
		shared_lot_number = AddAuctionOfLot.shared_lot_number_from_lno(lno)
		for p in prices:
			n = p.get('price_note')
			if n and n.startswith('for lots '):
				return n[9:]
		return shared_lot_number

	@staticmethod
	def transaction_uri_for_lot(data, prices):
		'''
		Return a URI representing the procurement which the object (identified by the
		supplied data) is a part of. This may identify just the lot being sold or, in the
		case of multiple lots being bought for a single price, a single procurement that
		encompasses multiple acquisitions that span different lots.
		'''
		cno, lno, date = object_key(data)
		shared_lot_number = AddAuctionOfLot.shared_lot_number_from_lno(lno)
		for p in prices:
			n = p.get('price_note')
			if n and n.startswith('for lots '):
				return pir_uri('AUCTION-TX-MULTI', cno, date, n[9:])
		return pir_uri('AUCTION-TX', cno, date, shared_lot_number)

	@staticmethod
	def set_lot_notes(lot, auction_data):
		'''Associate notes with the auction lot.'''
		cno, lno, _ = object_key(auction_data)
		auction, _, _ = auction_event_for_catalog_number(cno)
		notes = auction_data.get('lot_notes')
		if notes:
			note_id = lot.id + '-LotNotes'
			lot.referred_to_by = vocab.Note(ident=note_id, content=notes)
		if not lno:
			warnings.warn(f'Setting empty identifier on {lot.id}')
		lno = str(lno)
		lot.identified_by = vocab.LotNumber(ident='', content=lno)
		lot.part_of = auction

	def set_lot_objects(self, lot, cno, lno, data):
		'''Associate the set of objects with the auction lot.'''
		coll = vocab.AuctionLotSet(ident=f'{data["uri"]}-Set')
		shared_lot_number = self.shared_lot_number_from_lno(lno)
		coll._label = f'Auction Lot {cno} {shared_lot_number}'
		est_price = data.get('estimated_price')
		if est_price:
			coll.dimension = get_crom_object(est_price)
		start_price = data.get('start_price')
		if start_price:
			coll.dimension = get_crom_object(start_price)

		lot.used_specific_object = coll
		data['_lot_object_set'] = add_crom_data(data={}, what=coll)

	def __call__(self, data, non_auctions, auction_houses, auction_locations, problematic_records):
		'''Add modeling data for the auction of a lot of objects.'''
		ask_price = data.get('ask_price', {}).get('ask_price')
		if ask_price:
			# if there is an asking price/currency, it's a direct sale, not an auction;
			# filter these out from subsequent modeling of auction lots.
			return

		copy_source_information(data['_object'], data)

		auction_data = data['auction_of_lot']
		try:
			lot_object_key = object_key(auction_data)
		except Exception as e:
			warnings.warn(f'Failed to compute lot object key from data {auction_data} ({e})')
			pprint.pprint({k: v for k, v in data.items() if v != ''})
			raise
		cno, lno, date = lot_object_key
		if cno in non_auctions:
			# the records in this sales catalog do not represent auction sales, so should
			# be skipped.
			return

		shared_lot_number = self.shared_lot_number_from_lno(lno)
		uid, uri = self.shared_lot_number_ids(cno, lno, date)
		data['uid'] = uid
		data['uri'] = uri

		lot = vocab.Auction(ident=data['uri'])
		lot_id = f'{cno} {shared_lot_number} ({date})'
		lot_object_id = f'{cno} {lno} ({date})'
		lot_label = f'Auction of Lot {lot_id}'
		lot._label = lot_label
		data['lot_id'] = lot_id
		data['lot_object_id'] = lot_object_id

		for problem_key, problem in problematic_records.get('lots', []):
			# TODO: this is inefficient, but will probably be OK so long as the number
			#       of problematic records is small. We do it this way because we can't
			#       represent a tuple directly as a JSON dict key, and we don't want to
			#       have to do post-processing on the services JSON files after loading.
			if tuple(problem_key) == lot_object_key:
				note = model.LinguisticObject(ident='', content=problem)
				note.classified_as = vocab.instances["brief text"]
				note.classified_as = model.Type(
					ident=PROBLEMATIC_RECORD_URI,
					label='Problematic Record'
				)
				lot.referred_to_by = note

		self.set_lot_auction_houses(lot, cno, auction_houses)
		self.set_lot_location(lot, cno, auction_locations)
		self.set_lot_date(lot, auction_data)
		self.set_lot_notes(lot, auction_data)
		self.set_lot_objects(lot, cno, lno, data)

		tx_uri = AddAuctionOfLot.transaction_uri_for_lot(auction_data, data.get('price', []))
		lots = AddAuctionOfLot.lots_in_transaction(auction_data, data.get('price', []))
		multi = AddAuctionOfLot.transaction_contains_multiple_lots(auction_data, data.get('price', []))
		tx = vocab.Procurement(ident=tx_uri)
		tx._label = f'Procurement of Lot {cno} {lots} ({date})'
		lot.caused = tx
		tx_data = {'uri': tx_uri}

		if multi:
			tx_data['multi_lot_tx'] = lots
		with suppress(AttributeError):
			tx_data['_date'] = lot.timespan
		data['_procurement_data'] = add_crom_data(data=tx_data, what=tx)

		add_crom_data(data=data, what=lot)
		yield data

def add_crom_price(data, parent, services):
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
		amnt = extract_monetary_amount(data, currency_mapping=c)
	else:
		amnt = extract_monetary_amount(data, currency_mapping=currencies)
	if amnt:
		add_crom_data(data=data, what=amnt)
	return data

@use('make_la_person')
def add_person(data: dict, sales_record, rec_id, *, make_la_person):
	'''
	Add modeling data for people, based on properties of the supplied `data` dict.

	This function adds properties to `data` before calling
	`pipeline.linkedart.MakeLinkedArtPerson` to construct the model objects.
	'''
	
	pi = PersonIdentity()
	pi.add_uri(data, record_id=rec_id)
	pi.add_names(data, referrer=sales_record)

	make_la_person(data)
	return data

def final_owner_procurement(final_owner, current_tx, hmo, current_ts):
	tx = related_procurement(current_tx, hmo, current_ts, buyer=final_owner)
	try:
		object_label = hmo._label
		tx._label = f'Procurement leading to the currently known location of “{object_label}”'
	except AttributeError:
		tx._label = f'Procurement leading to the currently known location of object'
	return tx

@use('make_la_person')
def add_acquisition(data, buyers, sellers, buy_sell_modifiers, make_la_person=None):
	'''Add modeling of an acquisition as a transfer of title from the seller to the buyer'''
	sales_record = get_crom_object(data['_record'])
	hmo = get_crom_object(data)
	parent = data['parent_data']
# 	transaction = parent['transaction']
	prices = parent['price']
	auction_data = parent['auction_of_lot']
	cno, lno, date = object_key(auction_data)
	data['buyer'] = buyers
	data['seller'] = sellers
	
	acq_label = None
	try:
		object_label = f'“{hmo._label}”'
		acq_label = f'Acquisition of {cno} {lno} ({date}): {object_label}'
	except AttributeError:
		object_label = '(object)'
		acq_label = f'Acquisition of {cno} {lno} ({date})'
	amnts = [get_crom_object(p) for p in prices]

# 	if not prices:
# 		print(f'*** No price data found for {transaction} transaction')

	tx_data = parent['_procurement_data']
	current_tx = get_crom_object(tx_data)
	payment_id = current_tx.id + '-Payment'

	acq_id = hmo.id + '-Acquisition'
	acq = model.Acquisition(ident=acq_id, label=acq_label)
	acq.transferred_title_of = hmo

	multi = tx_data.get('multi_lot_tx')
	paym_label = f'multiple lots {multi}' if multi else object_label
	paym = model.Payment(ident=payment_id, label=f'Payment for {paym_label}')

	THROUGH = set(buy_sell_modifiers['through'])
	FOR = set(buy_sell_modifiers['for'])

	for seller_data in sellers:
		seller = get_crom_object(seller_data)
		mod = seller_data.get('auth_mod_a', '')

		if 'or' == mod:
			mod_non_auth = seller_data.get('auth_mod')
			if mod_non_auth:
				acq.referred_to_by = vocab.Note(ident='', label=f'Seller modifier', content=mod_non_auth)
			warnings.warn('Handle OR buyer modifier') # TODO: some way to model this uncertainty?

		if mod in THROUGH:
			acq.carried_out_by = seller
			paym.carried_out_by = seller
		elif mod in FOR:
			acq.transferred_title_from = seller
			paym.paid_to = seller
		else:
			# covers non-modified
			acq.carried_out_by = seller
			acq.transferred_title_from = seller
			paym.carried_out_by = seller
			paym.paid_to = seller

	for buyer_data in buyers:
		buyer = get_crom_object(buyer_data)
		mod = buyer_data.get('auth_mod_a', '')
		
		if 'or' == mod:
			# or/or others/or another
			mod_non_auth = buyer_data.get('auth_mod')
			if mod_non_auth:
				acq.referred_to_by = vocab.Note(ident='', label=f'Buyer modifier', content=mod_non_auth)
			warnings.warn(f'Handle buyer modifier: {mod}') # TODO: some way to model this uncertainty?

		if mod in THROUGH:
			acq.carried_out_by = buyer
			paym.carried_out_by = buyer
		elif mod in FOR:
			acq.transferred_title_to = buyer
			paym.paid_from = buyer
		else:
			# covers non-modified
			acq.carried_out_by = buyer
			acq.transferred_title_to = buyer
			paym.carried_out_by = buyer
			paym.paid_from = buyer

	if len(amnts) > 1:
		warnings.warn(f'Multiple Payment.paid_amount values for object {hmo.id} ({payment_id})')
	for amnt in amnts:
		paym.paid_amount = amnt
		break # TODO: sensibly handle multiplicity in paid amount data

	ts = tx_data.get('_date')
	if ts:
		acq.timespan = ts
	current_tx.part = paym
	current_tx.part = acq
	if '_procurements' not in data:
		data['_procurements'] = []
	data['_procurements'] += [add_crom_data(data={}, what=current_tx)]
# 	lot_uid, lot_uri = AddAuctionOfLot.shared_lot_number_ids(cno, lno)
	# TODO: `annotation` here is from add_physical_catalog_objects
# 	paym.referred_to_by = annotation

	data['_acquisition'] = add_crom_data(data={'uri': acq_id}, what=acq)

	final_owner_data = data.get('_final_org', [])
	if final_owner_data:
		data['_organizations'].append(final_owner_data)
		final_owner = get_crom_object(final_owner_data)
		tx = final_owner_procurement(final_owner, current_tx, hmo, ts)
		data['_procurements'].append(add_crom_data(data={}, what=tx))

	post_own = data.get('post_owner', [])
	prev_own = data.get('prev_owner', [])
	prev_post_owner_records = [(post_own, False), (prev_own, True)]
	for owner_data, rev in prev_post_owner_records:
		if rev:
			rev_name = 'prev-owner'
			source_label = 'Source of information on history of the object prior to the current sale.'
		else:
			rev_name = 'post-owner'
			source_label = 'Source of information on history of the object after the current sale.'
		for seq_no, owner_record in enumerate(owner_data):
			ignore_fields = ('own_so', 'own_auth_l', 'own_auth_d')
			if not any([bool(owner_record.get(k)) for k in owner_record.keys() if k not in ignore_fields]):
				# some records seem to have metadata (source information, location, or notes) but no other fields set
				# these should not constitute actual records of a prev/post owner.
				continue
			owner_record.update({
				'pi_record_no': data['pi_record_no'],
				'ulan': owner_record['own_ulan'],
				'auth_name': owner_record['own_auth'],
				'name': owner_record['own']
			})
			pi = PersonIdentity()
			pi.add_uri(owner_record, record_id=f'{rev_name}-{seq_no+1}')
			pi.add_names(owner_record, referrer=sales_record, role='artist')
			make_la_person(owner_record)
			owner = get_crom_object(owner_record)
			
			# TODO: handle other fields of owner_record: own_auth_d, own_auth_q, own_ques, own_so
			
			if owner_record.get('own_auth_l'):
				loc = owner_record['own_auth_l']
				current = parse_location_name(loc, uri_base=UID_TAG_PREFIX)
				place_data = pipeline.linkedart.make_la_place(current)
				place = get_crom_object(place_data)
				owner.residence = place
				data['_owner_locations'].append(place_data)
			
			if '_other_owners' not in data:
				data['_other_owners'] = []
			data['_other_owners'].append(owner_record)

			tx = related_procurement(current_tx, hmo, ts, buyer=owner, previous=rev)

			own_info_source = owner_record.get('own_so')
			if own_info_source:
				note = vocab.SourceStatement(content=own_info_source, label=source_label)
				tx.referred_to_by = note

			ptx_data = tx_data.copy()
			data['_procurements'].append(add_crom_data(data=ptx_data, what=tx))
	yield data

def related_procurement(current_tx, hmo, current_ts=None, buyer=None, seller=None, previous=False):
	'''
	Returns a new `vocab.Procurement` object (and related acquisition) that is temporally
	related to the supplied procurement and associated data. The new procurement is for
	the given object, and has the given buyer and seller (both optional).

	If the `previous` flag is `True`, the new procurement is occurs before `current_tx`,
	and if the timespan `current_ts` is given, has temporal data to that effect. If
	`previous` is `False`, this relationship is reversed.
	'''
	tx = vocab.Procurement()
	if current_tx:
		if previous:
			tx.ends_before_the_start_of = current_tx
		else:
			tx.starts_after_the_end_of = current_tx
	modifier_label = 'Previous' if previous else 'Subsequent'
	try:
		pacq = model.Acquisition(label=f'{modifier_label} Acquisition of: “{hmo._label}”')
	except AttributeError:
		pacq = model.Acquisition(label=f'{modifier_label} Acquisition')
	pacq.transferred_title_of = hmo
	if buyer:
		pacq.transferred_title_to = buyer
	if seller:
		pacq.transferred_title_from = seller
	tx.part = pacq
	if current_ts:
		if previous:
			pacq.timespan = timespan_before(current_ts)
		else:
			pacq.timespan = timespan_after(current_ts)
	return tx

def add_bidding(data, buyers, buy_sell_modifiers):
	'''Add modeling of bids that did not lead to an acquisition'''
	parent = data['parent_data']
	prices = parent['price']
	amnts = [get_crom_object(p) for p in prices]

	if amnts:
		auction_data = parent['auction_of_lot']
		cno, lno, date = object_key(auction_data)
		lot = get_crom_object(parent)
		hmo = get_crom_object(data)
		bidding_id = hmo.id + '-Bidding'
		all_bids = model.Activity(ident=bidding_id, label=f'Bidding on {cno} {lno} ({date})')

		all_bids.part_of = lot

		THROUGH = set(buy_sell_modifiers['through'])
		FOR = set(buy_sell_modifiers['for'])

		for seq_no, amnt in enumerate(amnts):
			bid_id = hmo.id + f'-Bid-{seq_no}'
			bid = vocab.Bidding(ident=bid_id)
			prop_id = hmo.id + f'-Bid-{seq_no}-Promise'
			try:
				amnt_label = amnt._label
				bid._label = f'Bid of {amnt_label} on {cno} {lno} ({date})'
				prop = model.PropositionalObject(ident=prop_id, label=f'Promise to pay {amnt_label}')
			except AttributeError:
				bid._label = f'Bid on {cno} {lno} ({date})'
				prop = model.PropositionalObject(ident=prop_id, label=f'Promise to pay')

			prop.refers_to = amnt
			bid.created = prop

			# TODO: there are often no buyers listed for non-sold records.
			#       should we construct an anonymous person to carry out the bid?
			for buyer_data in buyers:
				buyer = get_crom_object(buyer_data)
				mod = buyer_data.get('auth_mod_a', '')
				if mod in THROUGH:
					bid.carried_out_by = buyer
				elif mod in FOR:
					warnings.warn(f'buyer modifier {mod} for non-sale bidding: {cno} {lno} {date}')
					pass
				else:
					bid.carried_out_by = buyer

			all_bids.part = bid

		final_owner_data = data.get('_final_org')
		if final_owner_data:
			data['_organizations'].append(final_owner_data)
			final_owner = get_crom_object(final_owner_data)
			ts = lot.timespan
			hmo = get_crom_object(data)
			tx = final_owner_procurement(final_owner, None, hmo, ts)
			if '_procurements' not in data:
				data['_procurements'] = []
			data['_procurements'].append(add_crom_data(data={}, what=tx))

		data['_bidding'] = {'uri': bidding_id}
		add_crom_data(data=data['_bidding'], what=all_bids)
		yield data
	else:
		warnings.warn(f'*** No price data found for {parent["transaction"]!r} transaction')
		yield data

@use('buy_sell_modifiers')
@use('make_la_person')
def add_acquisition_or_bidding(data, *, buy_sell_modifiers, make_la_person):
	'''Determine if this record has an acquisition or bidding, and add appropriate modeling'''
	parent = data['parent_data']
	sales_record = get_crom_object(data['_record'])
	transaction = parent['transaction']
	transaction = transaction.replace('[?]', '').rstrip()

	buyers = [
		add_person(
			copy_source_information(p, parent),
			sales_record,
			f'buyer_{i+1}',
			make_la_person=make_la_person
		) for i, p in enumerate(parent['buyer'])
	]

	# TODO: is this the right set of transaction types to represent acquisition?
	if transaction in ('Sold', 'Vendu', 'Verkauft', 'Bought In'):
		sellers = [
			add_person(
				copy_source_information(p, parent),
				sales_record,
				f'seller_{i+1}',
				make_la_person=make_la_person
			) for i, p in enumerate(parent['seller'])
		]
		data['_owner_locations'] = []
		yield from add_acquisition(data, buyers, sellers, buy_sell_modifiers, make_la_person)
	elif transaction in ('Unknown', 'Unbekannt', 'Inconnue', 'Withdrawn', 'Non Vendu', ''):
		yield from add_bidding(data, buyers, buy_sell_modifiers)
	else:
		warnings.warn(f'Cannot create acquisition data for unknown transaction type: {transaction!r}')

#mark - Single Object Lot Tracking

class TrackLotSizes(Configurable):
	lot_counter = Service('lot_counter')

	def __call__(self, data, lot_counter):
		auction_data = data['auction_of_lot']
		cno, lno, date = object_key(auction_data)
		lot = AddAuctionOfLot.shared_lot_number_from_lno(lno)
		key = (cno, lot, date)
		lot_counter[key] += 1

#mark - Auction of Lot - Physical Object

def genre_instance(value, vocab_instance_map):
	'''Return the appropriate type instance for the supplied genre name'''
	if value is None:
		return None
	value = value.lower()

	instance_name = vocab_instance_map.get(value)
	if instance_name:
		instance = vocab.instances.get(instance_name)
		if not instance:
			warnings.warn(f'*** No genre instance available for {instance_name!r} in vocab_instance_map')
		return instance
	return None

def populate_destruction_events(data, note, *, type_map, location=None):
	destruction_types_map = type_map
	hmo = get_crom_object(data)
	title = data.get('title')

	r = re.compile(r'[Dd]estroyed(?: (?:by|during) (\w+))?(?: in (\d{4})[.]?)?')
	m = r.search(note)
	if m:
		method = m.group(1)
		year = m.group(2)
		dest_id = hmo.id + '-Destruction'
		d = model.Destruction(ident=dest_id, label=f'Destruction of “{title}”')
		d.referred_to_by = vocab.Note(ident='', content=note)
		if year is not None:
			begin, end = date_cleaner(year)
			ts = timespan_from_outer_bounds(begin, end)
			ts.identified_by = model.Name(ident='', content=year)
			d.timespan = ts

		if method:
			with suppress(KeyError, AttributeError):
				type_name = destruction_types_map[method.lower()]
				type = vocab.instances[type_name]
				event = model.Event(label=f'{method.capitalize()} event causing the destruction of “{title}”')
				event.classified_as = type
				d.caused_by = event

		if location:
			current = parse_location_name(location, uri_base=UID_TAG_PREFIX)
			base_uri = hmo.id + '-Place,'
			place_data = pipeline.linkedart.make_la_place(current, base_uri=base_uri)
			place = get_crom_object(place_data)
			if place:
				data['_locations'].append(place_data)
				d.took_place_at = place

		hmo.destroyed_by = d

@use('post_sale_map')
@use('unique_catalogs')
@use('vocab_instance_map')
@use('destruction_types_map')
def populate_object(data, post_sale_map, unique_catalogs, vocab_instance_map, destruction_types_map):
	'''Add modeling for an object described by a sales record'''
	hmo = get_crom_object(data)
	parent = data['parent_data']
	auction_data = parent.get('auction_of_lot')
	if auction_data:
		lno = str(auction_data['lot_number'])
		if 'identifiers' not in data:
			data['identifiers'] = []
		if not lno:
			warnings.warn(f'Setting empty identifier on {hmo.id}')
		data['identifiers'].append(vocab.LotNumber(ident='', content=lno))
	else:
		warnings.warn(f'***** NO AUCTION DATA FOUND IN populate_object')


	cno = auction_data['catalog_number']
	lno = auction_data['lot_number']
	date = implode_date(auction_data, 'lot_sale_')
	lot = AddAuctionOfLot.shared_lot_number_from_lno(lno)
	now_key = (cno, lot, date) # the current key for this object; may be associated later with prev and post object keys

	data['_locations'] = []
	record = _populate_object_catalog_record(data, parent, lot, cno, parent['pi_record_no'])
	_populate_object_visual_item(data, vocab_instance_map)
	_populate_object_destruction(data, parent, destruction_types_map)
	_populate_object_statements(data)
	_populate_object_present_location(data, now_key, destruction_types_map)
	_populate_object_notes(data, parent, unique_catalogs)
	_populate_object_prev_post_sales(data, now_key, post_sale_map)
	for p in data.get('portal', []):
		url = p['portal_url']
		hmo.referred_to_by = vocab.WebPage(ident=url)

	if 'title' in data:
		title = data['title']
		if not hasattr(hmo, '_label'):
			typestring = data.get('object_type', 'Object')
			hmo._label = f'{typestring}: “{title}”'
		del(data['title'])
		shorter = truncate_with_ellipsis(title, 100)
		if shorter:
			description = vocab.Description(ident='', content=title)
			description.referred_to_by = record
			hmo.referred_to_by = description
			title = shorter
		t = vocab.PrimaryName(ident='', content=title)
		t.classified_as = model.Type(ident='http://vocab.getty.edu/aat/300417193', label='Title')
		t.referred_to_by = record
		data['identifiers'].append(t)

	return data

def _populate_object_catalog_record(data, parent, lot, cno, rec_num):
	catalog_uri = pir_uri('CATALOG', cno)
	catalog = vocab.AuctionCatalogText(ident=catalog_uri)
	
	record_uri = pir_uri('CATALOG', cno, 'RECORD', rec_num)
	lot_object_id = parent['lot_object_id']
	record = model.LinguisticObject(ident=record_uri, label=f'Sale recorded in catalog: {lot_object_id} (record number {rec_num})') # TODO: needs classification
	record_data	= {'uri': record_uri}
	record_data['identifiers'] = [model.Name(ident='', content=f'Record of sale {lot_object_id}')]
	record.part_of = catalog

	data['_record'] = add_crom_data(data=record_data, what=record)
	return record

def _populate_object_destruction(data, parent, destruction_types_map):
	notes = parent.get('auction_of_lot', {}).get('lot_notes')
	if notes and notes.lower().startswith('destroyed'):
		populate_destruction_events(data, notes, type_map=destruction_types_map)

def _populate_object_visual_item(data, vocab_instance_map):
	hmo = get_crom_object(data)
	title = data.get('title')
	title = truncate_with_ellipsis(title, 100) or title

	vi_id = hmo.id + '-VisualItem'
	vi = model.VisualItem(ident=vi_id)
	vidata = {'uri': vi_id}
	if title:
		vidata['label'] = f'Visual work of “{title}”'
		sales_record = get_crom_object(data['_record'])
		vidata['names'] = [(title,{'referred_to_by': [sales_record]})]

	genre = genre_instance(data.get('genre'), vocab_instance_map)
	if genre:
		vi.classified_as = genre
	data['_visual_item'] = add_crom_data(data=vidata, what=vi)
	hmo.shows = vi

def _populate_object_statements(data):
	hmo = get_crom_object(data)
	materials = data.get('materials')
	if materials:
		matstmt = vocab.MaterialStatement(ident='', content=materials)
		sales_record = get_crom_object(data['_record'])
		matstmt.referred_to_by = sales_record
		hmo.referred_to_by = matstmt

	dimstr = data.get('dimensions')
	if dimstr:
		dimstmt = vocab.DimensionStatement(ident='', content=dimstr)
		sales_record = get_crom_object(data['_record'])
		dimstmt.referred_to_by = sales_record
		hmo.referred_to_by = dimstmt
		for dim in extract_physical_dimensions(dimstr):
			dim.referred_to_by = sales_record
			hmo.dimension = dim
	else:
		pass
# 		print(f'No dimension data was parsed from the dimension statement: {dimstr}')

def _populate_object_present_location(data, now_key, destruction_types_map):
	hmo = get_crom_object(data)
	location = data.get('present_location')
	if location:
		loc = location.get('geog')
		note = location.get('note')
		if loc:
			if 'destroyed ' in loc.lower():
				populate_destruction_events(data, loc, type_map=destruction_types_map)
			elif isinstance(note, str) and 'destroyed ' in note.lower():
				# the object was destroyed, so any "present location" data is actually
				# an indication of the location of destruction.
				populate_destruction_events(data, note, type_map=destruction_types_map, location=loc)
			else:
				# TODO: if `parse_location_name` fails, still preserve the location string somehow
				current = parse_location_name(loc, uri_base=UID_TAG_PREFIX)
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
						owner_data['uri'] = pir_uri('ORGANIZATION', 'ULAN', ulan)
					else:
						owner_data['uri'] = pir_uri('ORGANIZATION', 'NAME', inst, 'PLACE', loc)
				else:
					owner_data = {
						'label': '(Anonymous organization)',
						'uri': pir_uri('ORGANIZATION', 'PRESENT-OWNER', *now_key),
					}

				base_uri = hmo.id + '-Place,'
				place_data = pipeline.linkedart.make_la_place(current, base_uri=base_uri)
				place = get_crom_object(place_data)

				make_la_org = pipeline.linkedart.MakeLinkedArtOrganization()
				owner_data = make_la_org(owner_data)
				owner = get_crom_object(owner_data)
				owner.residence = place
				data['_locations'].append(place_data)
				data['_final_org'] = owner_data
		else:
			pass # there is no present location place string
		if note:
			pass
			# TODO: the acquisition_note needs to be attached as a Note to the final post owner acquisition

def _populate_object_notes(data, parent, unique_catalogs):
	hmo = get_crom_object(data)
	notes = data.get('hand_note', [])
	for note in notes:
		hand_note_content = note['hand_note']
		owner = note.get('hand_note_so')
		cno = parent['auction_of_lot']['catalog_number']
		catalog_uri = pir_uri('CATALOG', cno, owner, None)
		catalogs = unique_catalogs.get(catalog_uri)
		note = vocab.Note(ident='', content=hand_note_content)
		hmo.referred_to_by = note
		if catalogs and len(catalogs) == 1:
			note.carried_by = vocab.AuctionCatalog(ident=catalog_uri, label=f'Sale Catalog {cno}, owned by “{owner}”')

	inscription = data.get('inscription')
	if inscription:
		hmo.referred_to_by = vocab.InscriptionStatement(ident='', content=inscription)

def _populate_object_prev_post_sales(data, this_key, post_sale_map):
	post_sales = data.get('post_sale', [])
	prev_sales = data.get('prev_sale', [])
	prev_post_sales_records = [(post_sales, False), (prev_sales, True)]
	for sales_data, rev in prev_post_sales_records:
		for sale_record in sales_data:
			pcno = sale_record.get('cat')
			plno = sale_record.get('lot')
			plot = AddAuctionOfLot.shared_lot_number_from_lno(plno)
			pdate = implode_date(sale_record, '')
			if pcno and plot and pdate:
				that_key = (pcno, plot, pdate)
				if rev:
					# `that_key` is for a previous sale for this object
					post_sale_map[this_key] = that_key
				else:
					# `that_key` is for a later sale for this object
					post_sale_map[that_key] = this_key


@use('vocab_type_map')
def add_object_type(data, vocab_type_map):
	'''Add appropriate type information for an object based on its 'object_type' name'''
	typestring = data.get('object_type', '')
	if typestring in vocab_type_map:
		clsname = vocab_type_map.get(typestring, None)
		otype = getattr(vocab, clsname)
		add_crom_data(data=data, what=otype(ident=data['uri']))
	elif ';' in typestring:
		parts = [s.strip() for s in typestring.split(';')]
		if all([s in vocab_type_map for s in parts]):
			types = [getattr(vocab, vocab_type_map[s]) for s in parts]
			obj = vocab.make_multitype_obj(*types, ident=data['uri'])
			add_crom_data(data=data, what=obj)
		else:
			warnings.warn(f'*** Not all object types matched for {typestring!r}')
			add_crom_data(data=data, what=model.HumanMadeObject(ident=data['uri']))
	else:
		warnings.warn(f'*** No object type for {typestring!r}')
		add_crom_data(data=data, what=model.HumanMadeObject(ident=data['uri']))

	parent = data['parent_data']
	coll_data = parent.get('_lot_object_set')
	if coll_data:
		coll = get_crom_object(coll_data)
		if coll:
			data['member_of'] = [coll]

	return data

@use('attribution_modifiers')
@use('attribution_group_types')
@use('make_la_person')
def add_pir_artists(data, *, attribution_modifiers, attribution_group_types, make_la_person):
	'''Add modeling for artists as people involved in the production of an object'''
	hmo = get_crom_object(data)
	data['_organizations'] = []
	data['_original_objects'] = []
	
	try:
		hmo_label = f'{hmo._label}'
	except AttributeError:
		hmo_label = 'object'
	event_id = hmo.id + '-Production'
	event = model.Production(ident=event_id, label=f'Production event for {hmo_label}')
	hmo.produced_by = event

	artists = data.get('_artists', [])

	data['_artists'] = artists
	sales_record = get_crom_object(data['_record'])
	for seq_no, a in enumerate(artists):
		ulan = None
		with suppress(ValueError, TypeError):
			ulan = int(a.get('artist_ulan'))
		auth_name = a.get('art_authority')

		a.update({
			'pi_record_no': data['pi_record_no'],
			'ulan': a['artist_ulan'],
			'auth_name': a['art_authority'],
			'name': a['artist_name']
		})
		pi = PersonIdentity()
		pi.add_uri(a, record_id=f'artist-{seq_no+1}')
		pi.add_names(a, referrer=sales_record, role='artist')
		artist_label = a.get('role_label')
		make_la_person(a)
		person = get_crom_object(a)
		
		mod = a.get('attrib_mod_auth')
		if mod:
			mods = {m.lower().strip() for m in mod.split(';')}
			
			# TODO: this should probably be in its own JSON service file:
			STYLE_OF = set(attribution_modifiers['style of'])
			FORMERLY_ATTRIBUTED_TO = set(attribution_modifiers['formerly attributed to'])
			ATTRIBUTED_TO = set(attribution_modifiers['attributed to'])
			COPY_AFTER = set(attribution_modifiers['copy after'])
			PROBABLY = set(attribution_modifiers['probably by'])
			POSSIBLY = set(attribution_modifiers['possibly by'])
			UNCERTAIN = PROBABLY | POSSIBLY

			GROUP_TYPES = set(attribution_group_types.values())
			GROUP_MODS = {k for k, v in attribution_group_types.items() if v in GROUP_TYPES}
			
			if 'or' in mods:
				warnings.warn('Handle OR attribution modifier') # TODO: some way to model this uncertainty?
			
			if 'copy by' in mods:
				# equivalent to no modifier
				pass
			elif ATTRIBUTED_TO & mods:
				# equivalent to no modifier
				pass
			elif STYLE_OF & mods:
				assignment = model.AttributeAssignment(label=f'In the style of {artist_label}')
				event.attributed_by = assignment
				assignment.assigned_property = "influenced_by"
				assignment.property_classified_as = vocab.instances['style of']
				assignment.assigned = person
				continue
			elif GROUP_MODS & mods:
				mod_name = list(GROUP_MODS & mods)[0] # TODO: use all matching types?
				clsname = attribution_group_types[mod_name]
				cls = getattr(vocab, clsname)
				style_prod_uri = event_id + f'-style-{seq_no}'
				group_label = f'{clsname} of {artist_label}'
				group_id = a['uri'] + f'-{clsname}'
				group = cls(ident=group_id, label=group_label)
				formation = model.Formation(ident='', label=f'Formation of {group_label}')
				formation.influenced_by = person
				group.formed_by = formation
				group_data = add_crom_data({'uri': group_id}, group)
				data['_organizations'].append(group_data)
				subevent_id = event_id + f'-{seq_no}'
				subevent = model.Production(ident=subevent_id, label=f'Production sub-event for {group_label}')
				event.part = subevent
				subevent.carried_out_by = group
				continue
			elif FORMERLY_ATTRIBUTED_TO & mods:
				assignment = vocab.ObsoleteAssignment(ident='', label=f'Formerly attributed to {artist_label}')
				event.attributed_by = assignment
				assignment.assigned_property = "carried_out_by"
				assignment.assigned = person
				continue
			elif UNCERTAIN & mods:
				if POSSIBLY & mods:
					assignment = vocab.PossibleAssignment(ident='', label=f'Possibly attributed to {artist_label}')
					assignment._label = f'Possibly by {artist_label}'
				else:
					assignment = vocab.ProbableAssignment(ident='', label=f'Probably attributed to {artist_label}')
					assignment._label = f'Probably by {artist_label}'
				event.attributed_by = assignment
				assignment.assigned_property = "carried_out_by"
				assignment.assigned = person
				continue
			elif COPY_AFTER & mods:
				cls = type(hmo)
				original_id = hmo.id + '-Original'
				original_label = f'Original of {hmo_label}'
				original_hmo = cls(ident=original_id, label=original_label)
				original_event_id = original_hmo.id + '-Production'
				original_event = model.Production(ident=original_event_id, label=f'Production event for {original_label}')
				original_hmo.produced_by = original_event

				original_subevent_id = original_event_id + f'-{seq_no}'
				original_subevent = model.Production(ident=original_subevent_id, label=f'Production sub-event for {artist_label}')
				original_event.part = original_subevent
				original_subevent.carried_out_by = person

				event.influenced_by = original_hmo
				data['_original_objects'].append(add_crom_data(data={}, what=original_hmo))
				continue
			elif {'or', 'and'} & mods:
				pass
			else:
				print(f'UNHANDLED attrib_mod_auth VALUE: {mods}')
				pprint.pprint(a)
				continue
		
		subevent_id = event_id + f'-{seq_no}'
		subevent = model.Production(ident=subevent_id, label=f'Production sub-event for {artist_label}')
		event.part = subevent
		subevent.carried_out_by = person
	return data

#mark - Physical Catalogs

@use('non_auctions')
def add_auction_catalog(data, non_auctions):
	'''Add modeling for auction catalogs as linguistic objects'''
	cno = data['catalog_number']

	non_auction_flag = data.get('non_auction_flag')
	if non_auction_flag:
		non_auctions[cno] = non_auction_flag
	else:
		key = f'CATALOG-{cno}'
		cdata = {'uid': key, 'uri': pir_uri('CATALOG', cno)}
		catalog = vocab.AuctionCatalogText(ident=cdata['uri'])
		catalog._label = f'Sale Catalog {cno}'

		data['_catalog'] = add_crom_data(data=cdata, what=catalog)
		yield data

def add_physical_catalog_objects(data):
	'''Add modeling for physical copies of an auction catalog'''
	catalog = get_crom_object(data['_catalog'])
	cno = data['catalog_number']
	owner = data['owner_code']
	copy = data['copy_number']
	uri = pir_uri('CATALOG', cno, owner, copy)
	data['uri'] = uri
	labels = [f'Sale Catalog {cno}', f'owned by “{owner}”']
	if copy:
		labels.append(f'copy {copy}')
	catalogObject = vocab.AuctionCatalog(ident=uri, label=', '.join(labels))
	info = data.get('annotation_info')
	if info:
		catalogObject.referred_to_by = vocab.Note(ident='', content=info)
	catalogObject.carries = catalog

	add_crom_data(data=data, what=catalogObject)
	return data

@use('location_codes')
@use('unique_catalogs')
def add_physical_catalog_owners(data, location_codes, unique_catalogs):
	'''Add information about the ownership of a physical copy of an auction catalog'''
	# Add the URI of this physical catalog to `unique_catalogs`. This data will be used
	# later to figure out which catalogs can be uniquely identified by a catalog number
	# and owner code (e.g. for owners who do not have multiple copies of a catalog).
	cno = data['catalog_number']
	owner_code = data['owner_code']
	owner_name = None
	with suppress(KeyError):
		owner_name = location_codes[owner_code]
		owner_uri = pir_uri('ORGANIZATION', 'LOCATION-CODE', owner_code)
		data['_owner'] = {
			'label': owner_name,
			'uri': owner_uri,
			'identifiers': [
				model.Name(ident='', content=owner_name),
				model.Identifier(ident='', content=str(owner_code))
			],
		}
		owner = model.Group(ident=owner_uri)
		add_crom_data(data['_owner'], owner)
		if not owner_code:
			warnings.warn(f'Setting empty identifier on {owner.id}')
		add_crom_data(data=data['_owner'], what=owner)
		catalog = get_crom_object(data)
		catalog.current_owner = owner

	uri = pir_uri('CATALOG', cno, owner_code, None)
	if uri not in unique_catalogs:
		unique_catalogs[uri] = set()
	unique_catalogs[uri].add(uri)
	return data


#mark - Physical Catalogs - Informational Catalogs

def lugt_number_id(content, static_instances):
	lugt_number = str(content)
	lugt_id = vocab.LocalNumber(ident='', label=f'Lugt Number: {lugt_number}', content=lugt_number)
	assignment = model.AttributeAssignment(ident='')
	assignment.carried_out_by = static_instances.get_instance('Person', 'lugt')
	lugt_id.assigned_by = assignment
	return lugt_id

def gri_number_id(content, static_instances):
	catalog_id = vocab.LocalNumber(ident='', content=content)
	assignment = model.AttributeAssignment(ident='')
	assignment.carried_out_by = static_instances.get_instance('Group', 'gri')
	catalog_id.assigned_by = assignment
	return catalog_id

class PopulateAuctionCatalog(Configurable):
	'''Add modeling data for an auction catalog'''
	static_instances = Option(default="static_instances")

	def __call__(self, data):
		d = {k: v for k, v in data.items()}
		parent = data['parent_data']
		cno = str(parent['catalog_number'])
		sno = parent['star_record_no']
		catalog = get_crom_object(d)
		for lugt_no in parent.get('lugt', {}).values():
			if not lugt_no:
				warnings.warn(f'Setting empty identifier on {catalog.id}')
			catalog.identified_by = lugt_number_id(lugt_no, self.static_instances)

		if not cno:
			warnings.warn(f'Setting empty identifier on {catalog.id}')
		catalog.identified_by = gri_number_id(cno, self.static_instances)
	
		if not sno:
			warnings.warn(f'Setting empty identifier on {catalog.id}')
		catalog.identified_by = vocab.SystemNumber(ident='', content=sno)
		notes = data.get('notes')
		if notes:
			note = vocab.Note(ident='', content=parent['notes'])
			catalog.referred_to_by = note
		return d

#mark - Provenance Pipeline class

class ProvenancePipeline(PipelineBase):
	'''Bonobo-based pipeline for transforming Provenance data from CSV into JSON-LD.'''
	def __init__(self, input_path, catalogs, auction_events, contents, **kwargs):
		self.uid_tag_prefix = UID_TAG_PREFIX

		vocab.register_instance('fire', {'parent': model.Type, 'id': '300068986', 'label': 'Fire'})
		vocab.register_instance('animal', {'parent': model.Type, 'id': '300249395', 'label': 'Animal'})
		vocab.register_instance('history', {'parent': model.Type, 'id': '300033898', 'label': 'History'})
		vocab.register_vocab_class('AuctionCatalog', {'parent': model.HumanMadeObject, 'id': '300026068', 'label': 'Auction Catalog', 'metatype': 'work type'})

		super().__init__()
		self.project_name = 'provenance'
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
			add_auction_catalog,
			add_physical_catalog_objects,
			add_physical_catalog_owners,
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
			PopulateAuctionCatalog(static_instances=self.static_instances),
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
			add_auction_catalog,
			add_auction_event,
			add_auction_houses,
			populate_auction_event,
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
			self.add_serialization_chain(graph, p.output, model=self.models['Procurement'], use_memory_writer=False)

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
			add_acquisition_or_bidding,
			_input=sales.output
		)
		orgs = graph.add_chain(
			ExtractKeyedValues(key='_organizations'),
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
			self.add_serialization_chain(graph, bids.output, model=self.models['Activity'])
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
						'postprocess': lambda d, p: add_crom_price(d, p, services),
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
					'postprocess': add_pir_object_uri,
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
					'postprocess': lambda d, p: add_crom_price(d, p, services),
					'properties': (
						'est_price',
						'est_price_curr',
						'est_price_desc',
						'est_price_so')},
				'start_price': {
					'postprocess': lambda d, p: add_crom_price(d, p, services),
					'properties': (
						'start_price',
						'start_price_curr',
						'start_price_desc',
						'start_price_so')},
				'ask_price': {
					'postprocess': lambda d, p: add_crom_price(d, p, services),
					'properties': (
						'ask_price',
						'ask_price_curr',
						'ask_price_so')},
			}),
			AddAuctionOfLot(),
			_input=records.output
		)
		if serialize:
			# write SALES data
			self.add_serialization_chain(graph, sales.output, model=self.models['Activity'])
		return sales

	def add_single_object_lot_tracking_chain(self, graph, sales):
		small_lots = graph.add_chain(
			TrackLotSizes(),
			_input=sales.output
		)
		return small_lots

	def add_object_chain(self, graph, sales, serialize=True):
		'''Add modeling of the objects described by sales records.'''
		objects = graph.add_chain(
			ExtractKeyedValue(key='_object'),
			add_object_type,
			populate_object,
			pipeline.linkedart.MakeLinkedArtHumanMadeObject(),
			add_pir_artists,
			_input=sales.output
		)

		original_objects = graph.add_chain(
			ExtractKeyedValues(key='_original_objects'),
			_input=objects.output
		)
		if serialize:
			# write OBJECTS data
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
			src_uri = pir_uri('OBJECT', *src)
			dst_uri = pir_uri('OBJECT', *canonical)
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

	def persist_sales_tree(self, g):
		sales_tree_filename = os.path.join(settings.pipeline_tmp_path, 'sales-tree.data')
		with open(sales_tree_filename, 'w') as f:
			g.dump(f)

	def load_sales_tree(self):
		sales_tree_filename = os.path.join(settings.pipeline_tmp_path, 'sales-tree.data')
		if os.path.exists(sales_tree_filename):
			with open(sales_tree_filename) as f:
				g = SalesTree.load(f)
		else:
			g = SalesTree()
		return g

	def load_prev_post_sales_data(self):
		rewrite_map_filename = os.path.join(settings.pipeline_tmp_path, 'post_sale_rewrite_map.json')
		post_sale_rewrite_map = {}
		if os.path.exists(rewrite_map_filename):
			with open(rewrite_map_filename, 'r') as f:
				with suppress(json.decoder.JSONDecodeError):
					post_sale_rewrite_map = json.load(f)
		return post_sale_rewrite_map

	def persist_prev_post_sales_data(self, post_sale_rewrite_map):
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
