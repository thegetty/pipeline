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
import dateutil.parser
from collections import Counter, defaultdict, namedtuple
from contextlib import suppress
import inspect

import time
import timeit
from sqlalchemy import create_engine

import pipeline.execution

import graphviz
import bonobo
from bonobo.config import use, Option, Service, Configurable
from bonobo.nodes import Limit
from bonobo.constants import NOT_MODIFIED

import settings
from cromulent import model, vocab
from pipeline.projects import PipelineBase
from pipeline.projects.provenance.util import *
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
from cromulent.extract import extract_physical_dimensions, extract_monetary_amount
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

PROBLEMATIC_RECORD_URI = 'tag:getty.edu,2019:digital:pipeline:provenance:ProblematicRecord'
CSV_SOURCE_COLUMNS = ['pi_record_no', 'catalog_number']

IGNORE_PERSON_AUTHNAMES = CaseFoldingSet(('NEW', 'NON-UNIQUE'))
IGNORE_HOUSE_AUTHNAMES = CaseFoldingSet(('Anonymous',))

#mark - utility functions and classes

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
	country_name = data.get('country_auth_1')

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
	place_data = make_la_place(current)
	place = get_crom_object(place_data)
	if place:
		data['_locations'] = [place_data]
		auction.took_place_at = place
		auction_locations[cno] = place

	begin = implode_date(data, 'sale_begin_', clamp='begin')
	end = implode_date(data, 'sale_end_', clamp='end')
	ts = timespan_from_outer_bounds(
		begin=begin,
		end=end,
	)
	if begin and end:
		ts.identified_by = model.Name(ident='', content=f'{begin} – {end}')
	elif begin:
		ts.identified_by = model.Name(ident='', content=begin)
	elif end:
		ts.identified_by = model.Name(ident='', content=end)

	for p in data.get('portal', []):
		url = p['portal_url']
		auction.referred_to_by = vocab.WebPage(ident=url)

	if ts:
		auction.timespan = ts

	auction.subject_of = catalog
	return data

def add_auction_house_data(a):
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
		a['identifiers'].append(model.Identifier(ident='', content=str(ulan)))
		a['ulan'] = ulan
		house = vocab.AuctionHouseOrg(ident=a['uri'])
	elif auth_name and auth_name not in IGNORE_HOUSE_AUTHNAMES:
		a['uri'] = pir_uri('AUCTION-HOUSE', 'AUTHNAME', auth_name)
		a['identifiers'].append(
			vocab.PrimaryName(ident='', content=auth_name)
		)
		house = vocab.AuctionHouseOrg(ident=a['uri'])
	else:
		# TODO: should auc_house_auth be used here?
		# not enough information to identify this house uniquely, so use the source location in the input file
		a['uri'] = pir_uri('AUCTION-HOUSE', 'FILESOURCE', 'CATALOG-NUMBER', a['catalog_number'])
		house = vocab.AuctionHouseOrg(ident=a['uri'])

	name = a.get('auc_house_name', a.get('name'))
	if name:
		n = model.Name(content=name)
		n.referred_to_by = catalog
		a['identifiers'].append(n)
		a['label'] = name
	else:
		a['label'] = '(Anonymous)'

	auth = a.get('auc_house_auth')
	if auth:
		n = vocab.PrimaryName()
		n.content = auth
		a['identifiers'].append(n)

	add_crom_data(data=a, what=house)
	return a

@use('auction_houses')
def add_auction_houses(data, auction_houses):
	'''
	Add modeling data for the auction house organization(s) associated with an auction
	event.
	'''
	auction = get_crom_object(data)
	catalog = data['_catalog']['_LOD_OBJECT']
	d = data.copy()
	houses = data.get('auction_house', [])
	cno = data['catalog_number']

	house_objects = []

	for h in houses:
		h['_catalog'] = catalog
		add_auction_house_data(copy_source_information(h, data))
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
			end = implode_date(auction_data, 'lot_sale_', clamp='end')
			bounds = [begin, end]
		else:
			bounds = []
		if bounds:
			ts = timespan_from_outer_bounds(*bounds)
			ts.identified_by = model.Name(ident='', content=date)
			lot.timespan = ts

	@staticmethod
	def shared_lot_number_ids(cno, lno, date):
		shared_lot_number = AddAuctionOfLot.shared_lot_number_from_lno(lno)
		uid = f'AUCTION-{cno}-LOT-{shared_lot_number}-DATE-{date}'
		uri = pir_uri('AUCTION', cno, 'LOT', shared_lot_number, 'DATE', date)
		return uid, uri

	@staticmethod
	def transaction_uri_for_lot(data, prices):
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
		lot.identified_by = model.Identifier(ident='', content=str(lno))
		lot.part_of = auction

	def set_lot_objects(self, lot, lno, data):
		'''Associate the set of objects with the auction lot.'''
		coll = vocab.AuctionLotSet(ident=f'data["uri"]-Set')
		shared_lot_number = self.shared_lot_number_from_lno(lno)
		coll._label = f'Auction Lot {shared_lot_number}'
		est_price = data.get('estimated_price')
		if est_price:
			coll.dimension = get_crom_object(est_price)
		start_price = data.get('start_price')
		if start_price:
			coll.dimension = get_crom_object(start_price)

		lot.used_specific_object = coll
		data['_lot_object_set'] = coll

	def __call__(self, data, auction_houses, auction_locations, problematic_records):
		'''Add modeling data for the auction of a lot of objects.'''
		ask_price = data.get('ask_price', {}).get('ask_price')
		if ask_price:
			# if there is an asking price/currency, it's a direct sale, not an auction;
			# filter these out from subsequent modeling of auction lots.
			return
		
		copy_source_information(data['_object'], data)
		
		auction_data = data['auction_of_lot']
		lot_object_key = object_key(auction_data)
		cno, lno, date = lot_object_key
		shared_lot_number = self.shared_lot_number_from_lno(lno)
		uid, uri = self.shared_lot_number_ids(cno, lno, date)
		data['uid'] = uid
		data['uri'] = uri

		lot = vocab.Auction(ident=data['uri'])
		lot._label = f'Auction of Lot {cno} {shared_lot_number} ({date})'

		for problem_key, problem in problematic_records.get('lots', []):
			# TODO: this is inefficient, but will probably be OK so long as the number
			#       of problematic records is small. We do it this way because we can't
			#       represent a tuple directly as a JSON dict key, and we don't want to
			#       have to do post-processing on the services JSON files after loading.
			if tuple(problem_key) == lot_object_key:
				note = model.LinguisticObject(content=problem)
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
		self.set_lot_objects(lot, lno, data)
		
		tx_uri = AddAuctionOfLot.transaction_uri_for_lot(auction_data, data.get('price', []))
		tx = vocab.Procurement(ident=tx_uri)
		lot.caused = tx
		tx_data = {}
		with suppress(AttributeError):
			tx_data['_date'] = lot.timespan
		data['_procurement_data'] = add_crom_data(data=tx_data, what=tx)

		add_crom_data(data=data, what=lot)
		yield data

def _filter_empty_person(data: dict, _):
	'''
	If all the values of the supplied dictionary are false (or false after int conversion
	for keys ending with 'ulan'), return `None`. Otherwise return the dictionary.
	'''
	set = []
	for k, v in data.items():
		if k.endswith('ulan'):
			try:
				s = int(0)
			except ValueError:
				s = True
		else:
			s = bool(v)
		set.append(s)
	if any(set):
		return data
	else:
		return None

def add_crom_price(data, _):
	'''
	Add modeling data for `MonetaryAmount`, `StartingPrice`, or `EstimatedPrice`,
	based on properties of the supplied `data` dict.
	'''
	amnt = extract_monetary_amount(data)
	if amnt:
		add_crom_data(data=data, what=amnt)
	return data

@use('make_la_person')
def add_person(data: dict, rec_id, *, make_la_person):
	'''
	Add modeling data for people, based on properties of the supplied `data` dict.

	This function adds properties to `data` before calling
	`pipeline.linkedart.MakeLinkedArtPerson` to construct the model objects.
	'''
	ulan = None
	with suppress(ValueError, TypeError):
		ulan = int(data.get('ulan'))

	auth_name = data.get('auth_name')
	auth_name_q = '?' in data.get('auth_nameq', '')
	if ulan:
		key = f'PERSON-ULAN-{ulan}'
		data['uid'] = key
		data['uri'] = pir_uri('PERSON', 'ULAN', ulan)
		data['identifiers'] = [model.Identifier(ident='', content=str(ulan))]
		data['ulan'] = ulan
	elif auth_name and auth_name not in IGNORE_PERSON_AUTHNAMES and not(auth_name_q):
		data['uri'] = pir_uri('PERSON', 'AUTHNAME', auth_name)
		data['identifiers'] = [
			vocab.PrimaryName(ident='', content=auth_name) # NOTE: most of these are also vocab.SortName, but not 100%, so witholding that assertion for now
		]
	else:
		# not enough information to identify this person uniquely, so use the source location in the input file
		data['uri'] = pir_uri('PERSON', 'FILESOURCE', data['pi_record_no'], rec_id)

	names = []
	for name_string in set([data[k] for k in ('auth_name', 'name') if k in data and data[k]]):
		names.append((name_string,))
	if names:
		data['names'] = names
		data['label'] = names[0][0]
	else:
		data['label'] = '(Anonymous person)'

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
def add_acquisition(data, buyers, sellers, make_la_person=None):
	'''Add modeling of an acquisition as a transfer of title from the seller to the buyer'''
	hmo = get_crom_object(data)
	parent = data['parent_data']
	transaction = parent['transaction']
	prices = parent['price']
	auction_data = parent['auction_of_lot']
	cno, lno, date = object_key(auction_data)
	data['buyer'] = buyers
	data['seller'] = sellers
	object_label = hmo._label
	amnts = [get_crom_object(p) for p in prices]

# 	if not prices:
# 		print(f'*** No price data found for {transaction} transaction')

	tx_data = parent['_procurement_data']
	current_tx = get_crom_object(tx_data)
	payment_id = current_tx.id + '-Payment'

	acq = model.Acquisition(label=f'Acquisition of {cno} {lno} ({date}): “{object_label}”')
	acq.transferred_title_of = hmo
	paym = model.Payment(ident=payment_id, label=f'Payment for “{object_label}”')
	for seller in [get_crom_object(s) for s in sellers]:
		paym.paid_to = seller
		acq.transferred_title_from = seller
	for buyer in [get_crom_object(b) for b in buyers]:
		paym.paid_from = buyer
		acq.transferred_title_to = buyer
	for amnt in amnts:
		paym.paid_amount = amnt

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

	data['_acquisition'] = {}
	add_crom_data(data=data['_acquisition'], what=acq)

	final_owner_data = data.get('_final_org')
	if final_owner_data:
		final_owner = get_crom_object(final_owner_data)
		tx = final_owner_procurement(final_owner, current_tx, hmo, ts)
		data['_procurements'].append(add_crom_data(data={}, what=tx))

	post_own = data.get('post_owner', [])
	prev_own = data.get('prev_owner', [])
	prev_post_owner_records = [(post_own, False), (prev_own, True)]
	make_la_person = MakeLinkedArtPerson()
	for owner_data, rev in prev_post_owner_records:
		rev_name = 'prev-owner' if rev else 'post-owner'
		for rec_no, owner_record in enumerate(owner_data):
			name = owner_record.get('own_auth', owner_record.get('own'))
			owner_record['names'] = [(name,)]
			owner_record['label'] = name
			owner_record['uri'] = pir_uri('PERSON', 'FILESOURCE', data['pi_record_no'], f'{rev_name}-{rec_no+1}')
			# TODO: handle other fields of owner_record: own_auth_d, own_auth_l, own_auth_q, own_ques, own_so, own_ulan
			make_la_person(owner_record)
			owner = get_crom_object(owner_record)
			own_info_source = owner_record.get('own_so')
			if own_info_source:
				note = vocab.Note(content=own_info_source)
				hmo.referred_to_by = note
				owner.referred_to_by = note
			tx = related_procurement(current_tx, hmo, ts, buyer=owner, previous=rev)
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
	tx_data = {}
	if current_ts:
		if previous:
			pacq.timespan = timespan_before(current_ts)
		else:
			pacq.timespan = timespan_after(current_ts)
	return tx

def add_bidding(data, buyers):
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
			for buyer in [get_crom_object(b) for b in buyers]:
				bid.carried_out_by = buyer

			all_bids.part = bid

		final_owner_data = data.get('_final_org')
		if final_owner_data:
			final_owner = get_crom_object(final_owner_data)
			ts = lot.timespan
			hmo = get_crom_object(data)
			tx = final_owner_procurement(final_owner, None, hmo, ts)
			if '_procurements' not in data:
				data['_procurements'] = []
			data['_procurements'].append(add_crom_data(data={}, what=tx))

		data['_bidding'] = {}
		add_crom_data(data=data['_bidding'], what=all_bids)
		yield data
	else:
		pass
		warnings.warn(f'*** No price data found for {parent["transaction"]!r} transaction')

@use('make_la_person')
def add_acquisition_or_bidding(data, *, make_la_person):
	'''Determine if this record has an acquisition or bidding, and add appropriate modeling'''
	parent = data['parent_data']
	transaction = parent['transaction']
	transaction = transaction.replace('[?]', '').rstrip()

	buyers = [add_person(copy_source_information(p, parent), f'buyer_{i+1}', make_la_person=make_la_person) for i, p in enumerate(parent['buyer'])]

	# TODO: is this the right set of transaction types to represent acquisition?
	if transaction in ('Sold', 'Vendu', 'Verkauft', 'Bought In'):
		sellers = [add_person(copy_source_information(p, parent), f'seller_{i+1}', make_la_person=make_la_person) for i, p in enumerate(parent['seller'])]
		yield from add_acquisition(data, buyers, sellers, make_la_person)
	elif transaction in ('Unknown', 'Unbekannt', 'Inconnue', 'Withdrawn', 'Non Vendu', ''):
		yield from add_bidding(data, buyers)
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
			print(f'*** No genre instance available for {instance_name!r} in vocab_instance_map')
		return instance
	return None

def populate_destruction_events(data, note, destruction_types_map):
	hmo = get_crom_object(data)
	title = data.get('title')

	r = re.compile(r'Destroyed(?: (?:by|during) (\w+))?(?: in (\d{4})[.]?)?')
	m = r.search(note)
	if m:
		method = m.group(1)
		year = m.group(2)
		dest_id = hmo.id + '-Destruction'
		d = model.Destruction(ident=dest_id, label=f'Destruction of “{title}”')
		d.referred_to_by = vocab.Note(content=note)
		if year is not None:
			begin, end = date_cleaner(year)
			ts = timespan_from_outer_bounds(begin, end)
			ts.identified_by = model.Name(ident='', content=year)
			d.timespan = ts
		hmo.destroyed_by = d

		if method:
			with suppress(KeyError, AttributeError):
				type_name = destruction_types_map[method.lower()]
				type = vocab.instances[type_name]
				event = model.Event(label=f'{method.capitalize()} event causing the destruction of “{title}”')
				event.classified_as = type
				d.caused_by = event
	
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
		lno = auction_data['lot_number']
		if 'identifiers' not in data:
			data['identifiers'] = []
		if not lno:
			warnings.warn(f'Setting empty identifier on {hmo.id}')
		data['identifiers'].append(model.Identifier(ident='', content=str(lno)))
	else:
		print(f'***** NO AUCTION DATA FOUND IN populate_object')


	cno = auction_data['catalog_number']
	lno = auction_data['lot_number']
	date = implode_date(auction_data, 'lot_sale_')
	lot = AddAuctionOfLot.shared_lot_number_from_lno(lno)
	now_key = (cno, lot, date) # the current key for this object; may be associated later with prev and post object keys

	_populate_object_visual_item(data, vocab_instance_map)
	_populate_object_destruction(data, parent, destruction_types_map)
	_populate_object_statements(data)
	_populate_object_present_location(data, now_key, destruction_types_map)
	_populate_object_notes(data, parent, unique_catalogs)
	_populate_object_prev_post_sales(data, now_key, post_sale_map)
	for p in data.get('portal', []):
		url = p['portal_url']
		hmo.referred_to_by = vocab.WebPage(ident=url)

	return data

def _populate_object_destruction(data, parent, destruction_types_map):
	notes = parent.get('auction_of_lot', {}).get('lot_notes')
	if notes and notes.startswith('Destroyed'):
		populate_destruction_events(data, notes, destruction_types_map)

def _populate_object_visual_item(data, vocab_instance_map):
	hmo = get_crom_object(data)
	title = data.get('title')
	vidata = {}
	
	vi_id = hmo.id + '-VisualItem'
	vi = model.VisualItem(ident=vi_id)
	if title:
		vidata['label'] = f'Visual work of “{title}”'
		vidata['names'] = [(title,)]
	genre = genre_instance(data.get('genre'), vocab_instance_map)
	if genre:
		vi.classified_as = genre
	data['_visual_item'] = add_crom_data(data=vidata, what=vi)
	hmo.shows = vi

def _populate_object_statements(data):
	hmo = get_crom_object(data)
	m = data.get('materials')
	if m:
		matstmt = vocab.MaterialStatement(ident='')
		matstmt.content = m
		hmo.referred_to_by = matstmt

	dimstr = data.get('dimensions')
	if dimstr:
		dimstmt = vocab.DimensionStatement(ident='')
		dimstmt.content = dimstr
		hmo.referred_to_by = dimstmt
		for dim in extract_physical_dimensions(dimstr):
			hmo.dimension = dim
	else:
		pass
# 		print(f'No dimension data was parsed from the dimension statement: {dimstr}')

def _populate_object_present_location(data, now_key, destruction_types_map):
	location = data.get('present_location')
	if location:
		loc = location.get('geog')
		if loc:
			if 'Destroyed ' in loc:
				populate_destruction_events(data, loc, destruction_types_map)
			else:
				current = parse_location_name(loc, uri_base=UID_TAG_PREFIX)
				place_data = make_la_place(current)
				place = get_crom_object(place_data)
				# TODO: if `parse_location_name` fails, still preserve the location string somehow
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
				make_la_org = MakeLinkedArtOrganization()
				owner_data = make_la_org(owner_data)
				owner = get_crom_object(owner_data)
				owner.residence = place
				data['_locations'] = [place_data]
				data['_final_org'] = owner_data
		else:
			pass # there is no present location place string
		note = location.get('note')
		if note:
			pass
			# TODO: the acquisition_note needs to be attached as a Note to the final post owner acquisition

def _populate_object_notes(data, parent, unique_catalogs):
	hmo = get_crom_object(data)
	notes = data.get('hand_note', [])
	for note in notes:
		c = note['hand_note']
		owner = note.get('hand_note_so')
		cno = parent['auction_of_lot']['catalog_number']
		catalog_uri = pir_uri('CATALOG', cno, owner, None)
		catalogs = unique_catalogs.get(catalog_uri)
		note = vocab.Note(ident='', content=c)
		hmo.referred_to_by = note
		if catalogs and len(catalogs) == 1:
			note.carried_by = vocab.AuctionCatalog(ident=catalog_uri, label=f'Sale Catalog {cno}, owned by “{owner}”')

	inscription = data.get('inscription')
	if inscription:
		hmo.carries = vocab.Note(ident='', content=inscription)

def _populate_object_prev_post_sales(data, now_key, post_sale_map):
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
				later_key = (pcno, plot, pdate)
				if rev:
					later_key, now_key = now_key, later_key
				post_sale_map[later_key] = now_key


@use('vocab_type_map')
def add_object_type(data, vocab_type_map):
	'''Add appropriate type information for an object based on its 'object_type' name'''
	typestring = data.get('object_type', '')
	if typestring in vocab_type_map:
		clsname = vocab_type_map.get(typestring, None)
		otype = getattr(vocab, clsname)
		add_crom_data(data=data, what=otype(ident=data['uri']))
	else:
		print(f'*** No object type for {typestring!r}')
		add_crom_data(data=data, what=model.HumanMadeObject(ident=data['uri']))

	parent = data['parent_data']
	coll = parent.get('_lot_object_set')
	if coll:
		data['member_of'] = [coll]

	return data

@use('make_la_person')
def add_pir_artists(data, *, make_la_person):
	'''Add modeling for artists as people involved in the production of an object'''
	hmo = get_crom_object(data)
	try:
		hmo_label = f'“{hmo._label}”'
	except AttributeError:
		hmo_label = 'object'
	event_id = f'{data["uri"]}-Production'
	event = model.Production(ident=event_id, label=f'Production event for {hmo_label}')
	hmo.produced_by = event
	data['_production_event'] = add_crom_data({}, event)

	artists = data.get('_artists', [])

	data['_artists'] = artists
	for seq_no, a in enumerate(artists):
		# TODO: handle attrib_mod_auth field
# 		star_rec_no = None
# 		with suppress(ValueError, TypeError):
# 			star_rec_no = int(a.get('star_rec_no'))
		ulan = None
		with suppress(ValueError, TypeError):
			ulan = int(a.get('artist_ulan'))
		auth_name = a.get('art_authority')
		if ulan:
			key = f'PERSON-ULAN-{ulan}'
			a['uri'] = pir_uri('PERSON', 'ULAN', ulan)
			a['ulan'] = ulan
			a['uid'] = key
		elif auth_name and auth_name not in IGNORE_PERSON_AUTHNAMES:
			a['uri'] = pir_uri('PERSON', 'AUTHNAME', auth_name)
			a['identifiers'] = [
				vocab.PrimaryName(ident='', content=auth_name) # NOTE: most of these are also vocab.SortName, but not 100%, so witholding that assertion for now
			]
		else:
			# not enough information to identify this person uniquely, so use the source location in the input file
# 			warnings.warn(f'*** Person without a ulan or star number: {a}')
			a['uri'] = pir_uri('PERSON', 'FILESOURCE', data['pi_record_no'], f'artist-{seq_no+1}')

		names = []
		try:
			name = a['artist_name']
			names += [(name,)]
			a['label'] = name
		except KeyError:
			a['label'] = '(Anonymous artist)'

		a['names'] = names

		make_la_person(a)
		person = get_crom_object(a)
		subevent_id = event_id + f'-{seq_no}'
		subevent = model.Production(ident=subevent_id)
		event.part = subevent
		names = a.get('names')
		if names:
			name = names[0][0]
			subevent._label = f'Production sub-event for artist “{name}”'
		subevent.carried_out_by = person
	return data

#mark - Physical Catalogs

def add_auction_catalog(data):
	'''Add modeling for auction catalogs as linguistic objects'''
	cno = data['catalog_number']
	key = f'CATALOG-{cno}'
	cdata = {'uid': key, 'uri': pir_uri('CATALOG', cno)}
	catalog = vocab.AuctionCatalogText(ident=cdata['uri'])
	catalog._label = f'Sale Catalog {cno}'
	data['_catalog'] = cdata

	add_crom_data(data=cdata, what=catalog)
	return data

def add_physical_catalog_objects(data):
	'''Add modeling for physical copies of an auction catalog'''
	catalog = data['_catalog']['_LOD_OBJECT']
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

	# TODO: Rob's build-sample-auction-data.py script adds this annotation. where does it come from?
# 	anno = vocab.Annotation()
# 	anno._label = "Additional annotations in WSHC copy of BR-A1"
# 	catalogObject.carries = anno
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
		owner_data = add_crom_data(data=data['_owner'], what=owner)
		catalog = get_crom_object(data)
		catalog.current_owner = owner

	uri = pir_uri('CATALOG', cno, owner_code, None)
	if uri not in unique_catalogs:
		unique_catalogs[uri] = set()
	unique_catalogs[uri].add(uri)
	return data


#mark - Physical Catalogs - Informational Catalogs

def populate_auction_catalog(data):
	'''Add modeling data for an auction catalog'''
	d = {k: v for k, v in data.items()}
	parent = data['parent_data']
	cno = parent['catalog_number']
	sno = parent['star_record_no']
	catalog = get_crom_object(d)
	for lno in parent.get('lugt', {}).values():
		if not lno:
			warnings.warn(f'Setting empty identifier on {catalog.id}')
		catalog.identified_by = model.Identifier(ident='', label=f"Lugt Number: {lno}", content=str(lno))
	if not cno:
		warnings.warn(f'Setting empty identifier on {catalog.id}')
	catalog.identified_by = model.Identifier(ident='', content=str(cno))
	if not sno:
		warnings.warn(f'Setting empty identifier on {catalog.id}')
	catalog.identified_by = vocab.LocalNumber(ident='', content=sno)
	notes = data.get('notes')
	if notes:
		note = vocab.Note(ident='', content=parent['notes'])
		catalog.referred_to_by = note
	return d

#mark - Provenance Pipeline class

class ProvenancePipeline(PipelineBase):
	'''Bonobo-based pipeline for transforming Provenance data from CSV into JSON-LD.'''
	def __init__(self, input_path, catalogs, auction_events, contents, **kwargs):
		vocab.register_instance('fire', {'parent': model.Type, 'id': '300068986', 'label': 'Fire'})
		vocab.register_instance('animal', {'parent': model.Type, 'id': '300249395', 'label': 'Animal'})
		vocab.register_instance('history', {'parent': model.Type, 'id': '300033898', 'label': 'History'})
		vocab.register_vocab_class('SalesCatalog', {'parent': model.HumanMadeObject, 'id': '300026074', 'label': 'Sales Catalog'})
		vocab.register_vocab_class('AuctionCatalog', {'parent': model.HumanMadeObject, 'id': '300026068', 'label': 'Auction Catalog'})
		
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
			'make_la_person': MakeLinkedArtPerson(),
			'lot_counter': Counter(),
			'unique_catalogs': {},
			'post_sale_map': {},
			'auction_houses': {},
			'auction_locations': {},
		})
		return services

	def add_physical_catalog_owners_chain(self, graph, catalogs, serialize=True):
		'''Add modeling of physical copies of auction catalogs.'''
		groups = graph.add_chain(
			ExtractKeyedValue(key='_owner'),
			MakeLinkedArtOrganization(),
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

	def add_catalog_linguistic_objects(self, graph, events, serialize=True):
		'''Add modeling of auction catalogs as linguistic objects.'''
		los = graph.add_chain(
			ExtractKeyedValue(key='_catalog'),
			populate_auction_catalog,
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
							'country_auth_1',
							'country_auth_2',
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
			self.add_serialization_chain(graph, auction_events.output, model=self.models['Event'], use_memory_writer=False)
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

		sellers = graph.add_chain(
			ExtractKeyedValues(key='seller'),
			_input=acquisitions.output
		)

		if serialize:
			# write SALES data
			self.add_serialization_chain(graph, buyers.output, model=self.models['Person'])
			self.add_serialization_chain(graph, sellers.output, model=self.models['Person'])

	def add_acquisitions_chain(self, graph, sales, serialize=True):
		'''Add modeling of the acquisitions and bidding on lots being auctioned.'''
		bid_acqs = graph.add_chain(
			add_acquisition_or_bidding,
			_input=sales.output
		)
		orgs = graph.add_chain(
			ExtractKeyedValue(key='_final_org'),
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

		if serialize:
			# write SALES data
			self.add_serialization_chain(graph, acqs.output, model=self.models['Acquisition'])
			self.add_serialization_chain(graph, bids.output, model=self.models['Activity'])
			self.add_serialization_chain(graph, orgs.output, model=self.models['Group'])
		return bid_acqs

	def add_sales_chain(self, graph, records, serialize=True):
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
							_filter_empty_person,
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
							_filter_empty_person
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
						'postprocess': add_crom_price,
						'prefixes': (
							'price_amount',
							'price_currency',
							'price_note',
							'price_source',
							'price_citation')},
					'buyer': {
						'postprocess': [
							lambda x, _: strip_key_prefix('buy_', x),
							_filter_empty_person
						],
						'prefixes': (
							'buy_name',
							'buy_name_so',
							'buy_name_ques',
							'buy_name_cite',
							'buy_mod',
							'buy_auth_name',
							'buy_auth_nameQ',
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
					'postprocess': add_crom_price,
					'properties': (
						'est_price',
						'est_price_curr',
						'est_price_desc',
						'est_price_so')},
				'start_price': {
					'postprocess': add_crom_price,
					'properties': (
						'start_price',
						'start_price_curr',
						'start_price_desc',
						'start_price_so')},
				'ask_price': {
					'postprocess': add_crom_price,
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
			MakeLinkedArtHumanMadeObject(),
			add_pir_artists,
			_input=sales.output
		)
		
		if serialize:
			# write OBJECTS data
			self.add_serialization_chain(graph, objects.output, model=self.models['HumanMadeObject'])

		return objects

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

	def add_production_chain(self, graph, objects, serialize=True):
		'''Add transformation of production events to the bonobo pipeline.'''
		events = graph.add_chain(
			ExtractKeyedValue(key='_production_event'),
			_input=objects.output
		)
		if serialize:
			# write VISUAL ITEMS data
			self.add_serialization_chain(graph, events.output, model=self.models['Production'], use_memory_writer=False)
		return events

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

	def _construct_graph(self, single_graph=False):
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
			_ = self.add_catalog_linguistic_objects(g, auction_events, serialize=True)
			_ = self.add_auction_houses_chain(g, auction_events, serialize=True)
			_ = self.add_places_chain(g, auction_events, serialize=True)

		for g in component2:
			contents_records = g.add_chain(
				MatchingFiles(path='/', pattern=self.contents_files_pattern, fs='fs.data.provenance'),
				CurriedCSVReader(fs='fs.data.provenance', limit=self.limit),
				AddFieldNames(field_names=self.contents_headers),
			)
			sales = self.add_sales_chain(g, contents_records, serialize=True)
			_ = self.add_single_object_lot_tracking_chain(g, sales)
			objects = self.add_object_chain(g, sales, serialize=True)
			_ = self.add_places_chain(g, objects, serialize=True)
			acquisitions = self.add_acquisitions_chain(g, objects, serialize=True)
			self.add_buyers_sellers_chain(g, acquisitions, serialize=True)
			self.add_procurement_chain(g, acquisitions, serialize=True)
			_ = self.add_people_chain(g, objects, serialize=True)
			_ = self.add_visual_item_chain(g, objects, serialize=True)
			_ = self.add_production_chain(g, objects, serialize=True)

		if single_graph:
			self.graph_0 = graph0
		else:
			self.graph_1 = graph1
			self.graph_2 = graph2

	def get_graph(self):
		'''Return a single bonobo.Graph object for the entire pipeline.'''
		if not self.graph_0:
			self._construct_graph(single_graph=True)

		return self.graph_0

	def get_graph_1(self):
		'''Construct the bonobo pipeline to fully transform Provenance data from CSV to JSON-LD.'''
		if not self.graph_1:
			self._construct_graph()
		return self.graph_1

	def get_graph_2(self):
		'''Construct the bonobo pipeline to fully transform Provenance data from CSV to JSON-LD.'''
		if not self.graph_2:
			self._construct_graph()
		return self.graph_2

	def run(self, services=None, **options):
		'''Run the Provenance bonobo pipeline.'''
		print(f'- Limiting to {self.limit} records per file', file=sys.stderr)
		if not services:
			services = self.get_services(**options)

		start = timeit.default_timer()
		print('Running graph component 1...', file=sys.stderr)
		graph1 = self.get_graph_1(**options)
		self.run_graph(graph1, services=services)

		print('Running graph component 2...', file=sys.stderr)
		graph2 = self.get_graph_2(**options)
		self.run_graph(graph2, services=services)
		
		print(f'Pipeline runtime: {timeit.default_timer() - start}', file=sys.stderr)


class ProvenanceFilePipeline(ProvenancePipeline):
	'''
	Provenance pipeline with serialization to files based on Arches model and resource UUID.

	If in `debug` mode, JSON serialization will use pretty-printing. Otherwise,
	serialization will be compact.
	'''
	def __init__(self, input_path, catalogs, auction_events, contents, **kwargs):
		super().__init__(input_path, catalogs, auction_events, contents, **kwargs)
		self.writers = []
		debug = kwargs.get('debug', False)
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

	def merge_post_sale_objects(self, counter, post_map):
		singles = {k for k in counter if counter[k] == 1}
		multiples = {k for k in counter if counter[k] > 1}
		
		total = 0
		mapped = 0

		rewrite_map_filename = os.path.join(settings.pipeline_tmp_path, 'post_sale_rewrite_map.json')
		sales_tree_filename = os.path.join(settings.pipeline_tmp_path, 'sales-tree.data')

		if os.path.exists(sales_tree_filename):
			with open(sales_tree_filename) as f:
				g = SalesTree.load(f)
		else:
			g = SalesTree()

		for src, dst in post_map.items():
			total += 1
			if dst in singles:
				mapped += 1
				g.add_edge(src, dst)
			elif dst in multiples:
				pass
				print(f'  {src} maps to a MULTI-OBJECT lot')
			else:
				print(f'  {src} maps to an UNKNOWN lot')
		print(f'mapped {mapped}/{total} objects to a previous sale', file=sys.stderr)

		large_components = set(g.largest_component_canonical_keys(10))
		dot = graphviz.Digraph()
		
		node_id = lambda n: f'n{n!s}'
		for n, i in g.nodes.items():
			key, _ = g.canonical_key(n)
			if key in large_components:
				dot.node(node_id(i), str(n))
		
		post_sale_rewrite_map = {}
		if os.path.exists(rewrite_map_filename):
			with open(rewrite_map_filename, 'r') as f:
				with suppress(json.decoder.JSONDecodeError):
					post_sale_rewrite_map = json.load(f)
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

		dot_filename = os.path.join(settings.pipeline_tmp_path, 'sales.dot')
		dot.save(filename=dot_filename)
		with open(rewrite_map_filename, 'w') as f:
			json.dump(post_sale_rewrite_map, f)
			print(f'Saved post-sales rewrite map to {rewrite_map_filename}')
		with open(sales_tree_filename, 'w') as f:
			g.dump(f)

# 		r = JSONValueRewriter(post_sale_rewrite_map)
# 		rewrite_output_files(r)

	def run(self, **options):
		'''Run the Provenance bonobo pipeline.'''
		start = timeit.default_timer()
		services = self.get_services(**options)
		super().run(services=services, **options)
		
		count = len(self.writers)
		for i, w in enumerate(self.writers):
			print('[%d/%d] writers being flushed' % (i+1, count))
			if isinstance(w, MergingMemoryWriter):
				w.flush()

		print('====================================================')
		print('Running post-processing of post-sale data...')
		counter = services['lot_counter']
		post_map = services['post_sale_map']
		self.merge_post_sale_objects(counter, post_map)
		print(f'>>> {len(post_map)} post sales records')
		print('Total runtime: ', timeit.default_timer() - start)  
