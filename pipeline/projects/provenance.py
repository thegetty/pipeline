'''
Classes and utility functions for instantiating, configuring, and
running a bonobo pipeline for converting Provenance Index CSV data into JSON-LD.
'''

# PIR Extracters

import sys
import uuid
import csv
import pprint
import itertools

from sqlalchemy import create_engine

import bonobo
from bonobo.config import use
from bonobo.nodes import Limit

import settings
from cromulent import model, vocab
from pipeline.util import identity, ExtractKeyedValue, ExtractKeyedValues, MatchingFiles, Dimension
from pipeline.util.cleaners import dimensions_cleaner, normalize_dimension
from pipeline.io.file import MultiFileWriter, MergingFileWriter
# from pipeline.io.arches import ArchesWriter
from pipeline.linkedart import \
			add_crom_data, \
			MakeLinkedArtManMadeObject, \
			make_la_person
from pipeline.io.csv import CurriedCSVReader
from pipeline.nodes.basic import \
			AddFieldNames, \
			GroupRepeatingKeys, \
			GroupKeys, \
			add_uuid, \
			AddDataDependentArchesModel, \
			AddArchesModel, \
			Serializer, \
			Trace

legacyIdentifier = None # TODO: aat:LegacyIdentifier?
doiIdentifier = vocab.DoiIdentifier
variantTitleIdentifier = vocab.Identifier # TODO: aat for variant titles?

# utility functions

def filter_empty_people(*people):
	for p in people:
		keys = set(p.keys())
		data_keys = keys - {'_CROM_FACTORY', '_LOD_OBJECT', 'pi_record_no', 'star_rec_no', 'persistent_puid', 'parent_data'}
		d = {k: p[k] for k in data_keys if p[k] and p[k] != '0'}
		if d:
			yield p

def strip_key_prefix(prefix, value):
	d = {}
	for k, v in value.items():
		if k.startswith(prefix):
			d[k.replace(prefix, '', 1)] = v
		else:
			d[k] = v
	return d

def add_record_ids(data, parent):
	for p in ('pi_record_no', 'persistent_puid'):
		data[p] = parent.get(p)
	return data

def add_object_uuid(data, parent):
	add_record_ids(data, parent)
	pi_record_no = data['pi_record_no']
	data['uid'] = f'OBJECT-{pi_record_no}'
	data['uuid'] = str(uuid.uuid4())
	return data

def implode_date(data, prefix):
	year = data.get(f'{prefix}year')
	month = data.get('{prefix}month')
	day = data.get('{prefix}day')
	if year and month and day:
		return f'{year}-{month}-{day}'
	elif year and month:
		return f'{year}-{month}'
	elif year:
		return f'{year}'
	return None

def auction_event_for_catalog_number(cno):
	uid = f'AUCTION-EVENT-CATALOGNUMBER-{cno}'
	auction = vocab.AuctionEvent()
	auction._label = f"Auction Event for {cno}"
	return auction, uid

def add_auction_event(data):
	cno = data['catalog_number']
	auction, uid = auction_event_for_catalog_number(cno)
	data['uid'] = uid
	add_crom_data(data=data, what=auction)
	yield data

def populate_auction_event(data):
	auction = data['_LOD_OBJECT']
	catalog = data['_catalog']['_LOD_OBJECT']
	begin = implode_date(data, 'sale_begin_')
	end = implode_date(data, 'sale_end_')

	location_data = data['location']

	places = []
	place_names = []
	specific = location_data.get('specific_loc')
	city = location_data.get('city_of_sale')
	country = location_data.get('country_auth_1')
	if country:
		place_names.insert(0, country)
		p = model.Place(label=', '.join(place_names))
		p.classified_as = vocab.instances['nation']
		places.append(p)
		p.identified_by = model.Name(content=country)
	if city:
		place_names.insert(0, city)
		p = model.Place(label=', '.join(place_names))
		p.classified_as = vocab.instances['city']
		if places:
			p.part_of = places[-1]
		places.append(p)
		p.identified_by = model.Name(content=city)
	if specific:
		place_names.insert(0, specific)
		p = model.Place(label=', '.join(place_names))
		if places:
			p.part_of = places[-1]
		places.append(p)
		p.identified_by = model.Name(content=specific)
	if places:
		loc = places[-1]
		auction.took_place_at = loc

	# TODO: how can we associate this location with the lot auction,
	# which is produced on an entirely different bonobo graph chain?
# 	lot.took_place_at = loc

	if begin or end:
		ts = model.TimeSpan()
		if begin is not None:
			ts.begin_of_the_begin = begin
		if end is not None:
			ts.end_of_the_end = end
		auction.timespan = ts

	auction.subject_of = catalog
	yield data

@use('uuid_cache')
def add_auction_catalog(data, uuid_cache=None):
	cno = data['catalog_number']
	cdata = {'uid': f'CATALOG-{cno}'}
	add_uuid(cdata, uuid_cache)
	catalog = vocab.AuctionCatalog()
	catalog._label = f'Sale Catalog {cno}'
	data['_catalog'] = cdata

	add_crom_data(data=cdata, what=catalog)
	yield data

def add_auction_of_lot(data):
	auction_data = data['auction_of_lot']
	cno = auction_data['catalog_number']
	auction, _ = auction_event_for_catalog_number(cno)
	lno = auction_data['lot_number']

	lot = vocab.Auction()
	lot._label = f'Auction of Lot {cno} {lno}'
	data['uid'] = f'AUCTION-{cno}-LOT-{lno}'

	date = implode_date(auction_data, 'lot_sale_')
	if date:
		ts = model.TimeSpan()
		# TODO: expand this to day bounds
		ts.begin_of_the_begin = date
		name = model.Name()
		name.content = date
		ts.identified_by = name
		lot.timespan = ts
	notes = auction_data.get('lot_notes')
	if notes:
		note = vocab.Note()
		note.content = notes
		lot.referred_to_by = note
	lotid = model.Identifier()
	lotid.content = lno
	lot.identified_by = lotid
	lot.part_of = auction

	coll = vocab.AuctionLotSet()
	coll._label = f'Auction Lot {lno}'
	lot.used_specific_object = coll
	data['_lot_object_set'] = coll

	add_crom_data(data=data, what=lot)
# 	pprint.pprint(data)
	yield data

def populate_auction_catalog(data):
	d = {k: v for k, v in data.items()}
	parent = data['parent_data']
	cno = parent['catalog_number']
	sno = parent['star_record_no']
	catalog = d['_LOD_OBJECT']
	for lno in parent.get('lugt', {}).values():
		lugt = model.Identifier()
		lugt._label = f"Lugt Number: {lno}"
		lugt.content = lno
		catalog.identified_by = lugt
	local = model.Identifier()
	local.content = cno
	catalog.identified_by = local
	local2 = vocab.LocalNumber()
	local2.content = sno
	catalog.identified_by = local2
	notes = data.get('notes')
	if notes:
		note = vocab.Note()
		note.content = parent['notes']
		catalog.referred_to_by = note
	yield d

def add_physical_catalog_objects(data):
	catalog = data['_catalog']['_LOD_OBJECT']
	data['uuid'] = str(uuid.uuid4()) # this is a single pass, and will not be referenced again
	catalogObject = model.ManMadeObject()
	catalogObject._label = data.get('annotation_info')
	# TODO: link this with the vocab.AuctionCatalog
	catalogObject.carries = catalog
	# TODO: Rob's build-sample-auction-data.py script adds this annotation. where does it come from?
# 	anno = vocab.Annotation()
# 	anno._label = "Additional annotations in WSHC copy of BR-A1"
# 	catalogObject.carries = anno
	add_crom_data(data=data, what=catalogObject)
	yield data

def add_crom_price(data, parent):
	MAPPING = {
		'fl': 'de florins',
		'fl.': 'de florins',
		'pounds': 'gb pounds',
		'livres': 'fr livres',
		'guineas': 'gb guineas',
		'Reichsmark': 'de reichsmarks'
	}
	amnt = model.MonetaryAmount()
	price_amount = data.get('price_amount')
	price_currency = data.get('price_currency')
	if price_amount:
		try:
			price_amount = float(price_amount)
			amnt.value =  price_amount
		except ValueError:
			amnt._label = price_amount # TODO: is there a way to associate the value string with the MonetaryAmount in a meaningful way?
# 			print(f'*** Not a numeric price amount: {v}')
	if price_currency:
		if price_currency in MAPPING:
			price_currency = MAPPING[price_currency] # TODO: can this be done safely with the PIR data?
# 		print(f'*** CURRENCY: {currency}')
		if price_currency in vocab.instances:
			amnt.currency = vocab.instances[price_currency]
		else:
			print(f'*** No currency instance defined for {price_currency}')
	if price_amount and price_currency:
		amnt._label = f'{price_amount} {price_currency}'
	elif price_amount:
		amnt._label = f'{price_amount}'
	add_crom_data(data=data, what=amnt)
	return data

def add_person(a, uuid_cache):
	ulan = a.get('ulan')
	if ulan:
		a['uid'] = f'PERSON-ULAN-{ulan}'
		a['identifiers'] = [model.Identifier(content=ulan)]
	else:
		a['uuid'] = str(uuid.uuid4()) # not enough information to identify this person uniquely, so they get a UUID

	name = a.get('auth_name', a.get('name'))
	if name:
		a['names'] = [(name,)]
		a['label'] = name
	else:
		a['label'] = '(Anonymous person)'

	add_uuid(a, uuid_cache)
	make_la_person(a)
	person = a['_LOD_OBJECT']
	return a

@use('uuid_cache')
def add_acquisition(data, uuid_cache=None):
	parent = data['parent_data']
	transaction = parent['transaction']
	data = data.copy()
	object = data['_LOD_OBJECT']
	lot = parent['_LOD_OBJECT']
	prices = parent['price']
	amnts = [p['_LOD_OBJECT'] for p in prices]
	buyers = [add_person(p, uuid_cache) for p in filter_empty_people(*parent['buyer'])]
	sellers = [add_person(p, uuid_cache) for p in filter_empty_people(*parent['seller'])]
	if transaction in ('Sold', 'Vendu', 'Verkauft', 'Bought In'): # TODO: is this the right set of transaction types to represent acquisition?
		data['buyer'] = buyers
		data['seller'] = sellers
		if not prices:
			print(f'*** No price data found for {transaction} transaction')

		acq = model.Acquisition()
		acq.transferred_title_of = object
		paym = model.Payment()
		for seller in [s['_LOD_OBJECT'] for s in sellers]:
			paym.paid_to = seller
			acq.transferred_title_from = seller
		for buyer in [b['_LOD_OBJECT'] for b in buyers]:
			paym.paid_from = buyer
			acq.transferred_title_to = buyer
		for amnt in amnts:
			paym.paid_amount = amnt

 		# TODO: `annotation` here is from add_physical_catalog_objects
# 		paym.referred_to_by = annotation
# 		acq.part_of = lot
		add_crom_data(data=data, what=acq)

		yield data
	elif transaction in ('Unknown', 'Unbekannt', 'Inconnue', 'Withdrawn', 'Non Vendu'):
		bids = parent.get('bid', )
		if amnts:
			pass
		else:
			# TODO: there may be an `est_price` value. should it be recorded as a bid?
			print(f'*** No price data found for {transaction} transaction')

		lot = parent['_LOD_OBJECT']
		auction_data = parent['auction_of_lot']
		cno = auction_data['catalog_number']
		lno = auction_data['lot_number']
		all_bids = model.Activity()
		all_bids._label = f'Bidding on {cno} {lno}'
		all_bids.part_of = lot

		for amnt in amnts:
			bid = vocab.Bidding()
			amnt_label = amnt._label
			bid._label = f'Bid of {amnt_label} on {lno}'
			# TODO: there are often no buyers listed for non-sold records.
			#       should we construct an anonymous person to carry out the bid?
			for buyer in [b['_LOD_OBJECT'] for b in buyers]:
				bid.carried_out_by = buyer

			prop = model.PropositionalObject()
			prop._label = f'Promise to pay {amnt_label}'
			bid.created = prop

			all_bids.part = bid

		add_crom_data(data=data, what=all_bids)

		yield data
	else:
		print(f'Cannot create acquisition data for unknown transaction type: {transaction}')

def genre_instance(value):
	if value is None:
		return None
	value = value.lower()

	# TODO: are these OK AAT instances for these genres?
	ANIMALS = model.Type(ident='http://vocab.getty.edu/aat/300249395', label='Animals')
	HISTORY = model.Type(ident='http://vocab.getty.edu/aat/300033898', label='History')
	MAPPING = {
		'animals': ANIMALS,
		'tiere': ANIMALS,
		'stilleben': vocab.instances['style still life'],
		'still life': vocab.instances['style still life'],
		'portraits': vocab.instances['style portrait'],
		'porträt': vocab.instances['style portrait'],
		'historie': HISTORY,
		'history': HISTORY,
		'landscape': vocab.instances['style landscape'],
		'landschaft': vocab.instances['style landscape'],
	}
	return MAPPING.get(value)

def populate_object(data):
	object = data['_LOD_OBJECT']
	m = data.get('materials')
	if m:
		matstmt = vocab.MaterialStatement()
		matstmt.content = m
		object.referred_to_by = matstmt

	title = data.get('title')
	vi = model.VisualItem()
	if title:
		vi._label = f'Visual work of {title}'
	genre = genre_instance(data.get('genre'))
	if genre:
		vi.classified_as = genre
	object.shows = vi

	dimstr = data.get('dimensions')
	if dimstr:
		dimstmt = vocab.DimensionStatement()
		dimstmt.content = dimstr
		object.referred_to_by = dimstmt
		dimensions = dimensions_cleaner(dimstr)
		if dimensions:
			for d in dimensions:
				d = normalize_dimension(d)
# 				print(f'Dimension {data["dimensions"]}: {d}')
				if d:
					if d.which == 'height':
						dim = vocab.Height()
					elif d.which == 'width':
						dim = vocab.Width()
					else:
						dim = model.Dimension()
					dim.value = d.value
					dim.unit = vocab.instances[d.unit]
					object.dimension = dim
	yield data

def add_object_type(data):
	TYPES = { # TODO: should this be in settings (or elsewhere)?
		'dessin': vocab.Drawing,
		'drawing': vocab.Drawing,
		'émail': vocab.Enamel,
		'enamel': vocab.Enamel,
		'gemälde': vocab.Painting,
		'miniatur': vocab.Miniature,
		'miniature': vocab.Miniature,
		'painting': vocab.Painting,
		'peinture': vocab.Painting,
		'sculpture': vocab.Sculpture,
		'skulptur': vocab.Sculpture,
		'tapestry': vocab.Tapestry,
		'tapisserie': vocab.Tapestry,
		'zeichnung': vocab.Drawing,
		# TODO: these are the most common, but more should be added
	}
	typestring = data.get('object_type', '').lower()
	if typestring in TYPES:
		otype = TYPES[typestring]
		add_crom_data(data=data, what=otype())
	else:
		print(f'*** No object type for {typestring!r}')
		add_crom_data(data=data, what=model.ManMadeObject())

	parent = data['parent_data']
	coll = parent.get('_lot_object_set')
	if coll:
		data['member_of'] = [coll]

	return data

@use('uuid_cache')
def add_pir_artists(data, uuid_cache=None):
	lod_object = data['_LOD_OBJECT']
	event = model.Production()
	lod_object.produced_by = event

	artists = data.get('_artists', [])
	artists = list(filter_empty_people(*artists))
	data['_artists'] = artists
	for a in artists:
		star_rec_no = a.get('star_rec_no')
		a['uid'] = f'PERSON-star-{star_rec_no}'
		if a.get('artist_name'):
			name = a.get('artist_name')
			a['names'] = [(name,)]
			a['label'] = name
		else:
			a['label'] = '(Anonymous artist)'

		add_uuid(a, uuid_cache)
		make_la_person(a)
		person = a['_LOD_OBJECT']
		subevent = model.Creation()
		# TODO: The should really be asserted as object -created_by-> CreationEvent -part-> SubEvent
		# however, right now that assertion would get lost as it's data that belongs to the object,
		# and we're on the author's chain in the bonobo graph; object serialization has already happened.
		# we need to serialize the object's relationship to the creation event, and let it get merged
		# with the rest of the object's data.
		event.part = subevent
		names = a.get('names')
		if names:
			subevent._label = f'Creation sub-event for {names[0]}'
		subevent.carried_out_by = person
	yield data




#mark -



# Provenance Pipeline class

class ProvenancePipeline:
	'''Bonobo-based pipeline for transforming Provenance data from CSV into JSON-LD.'''
	def __init__(self, input_path, catalogs, auction_events, contents, **kwargs):
		self.graph = None
		self.models = kwargs.get('models', {})
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
			self.catalogs_headers = next(r)
		with fs.open(self.auction_events_header_file, newline='') as csvfile:
			r = csv.reader(csvfile)
			self.auction_events_headers = next(r)
		with fs.open(self.contents_header_file, newline='') as csvfile:
			r = csv.reader(csvfile)
			self.contents_headers = next(r)

		if self.debug:
			self.serializer	= Serializer(compact=False)
			self.writer		= None
			# self.writer	= ArchesWriter()
			sys.stderr.write("In DEBUGGING mode\n")
		else:
			self.serializer	= Serializer(compact=True)
			self.writer		= None
			# self.writer	= ArchesWriter()

	# Set up environment
	def get_services(self):
		'''Return a `dict` of named services available to the bonobo pipeline.'''
		return {
			'trace_counter': itertools.count(),
			'gpi': create_engine(settings.gpi_engine),
			'aat': create_engine(settings.aat_engine),
			'uuid_cache': create_engine(settings.uuid_cache_engine),
			'fs.data.pir': bonobo.open_fs(self.input_path)
		}

	def add_serialization_chain(self, graph, input_node):
		'''Add serialization of the passed transformer node to the bonobo graph.'''
		if self.writer is not None:
			graph.add_chain(
				self.serializer,
				self.writer,
				_input=input_node
			)
		else:
			sys.stderr.write('*** No serialization chain defined\n')

	def add_physical_catalogs_chain(self, graph, records, serialize=True):
		if self.limit is not None:
			records = graph.add_chain(
				Limit(self.limit),
				_input=records.output
			)
		catalogs = graph.add_chain(
			AddFieldNames(field_names=self.catalogs_headers),
			add_auction_catalog,
			add_physical_catalog_objects,
			AddDataDependentArchesModel(models=self.models),
# 			Trace(name='catalogs'),
			_input=records.output
		)
		if serialize:
			# write SALES data
			self.add_serialization_chain(graph, catalogs.output)
		return catalogs

	def add_catalog_linguistic_objects(self, graph, events, serialize=True):
		los = graph.add_chain(
			ExtractKeyedValue(key='_catalog'),
			populate_auction_catalog,
			add_uuid,
			AddDataDependentArchesModel(models=self.models),
			_input=events.output
		)
		if serialize:
			# write SALES data
			self.add_serialization_chain(graph, los.output)
		return los

	def add_auction_events_chain(self, graph, records, serialize=True):
		if self.limit is not None:
			records = graph.add_chain(
				Limit(self.limit),
				_input=records.output
			)
		auction_events = graph.add_chain(
			AddFieldNames(field_names=self.auction_events_headers),
			GroupRepeatingKeys(mapping={
				'seller': {'prefixes': ('sell_auth_name', 'sell_auth_q')},
				'expert': {'prefixes': ('expert', 'expert_auth', 'expert_ulan')},
				'commissaire': {'prefixes': ('comm_pr', 'comm_pr_auth', 'comm_pr_ulan')},
				'auction_house': {'prefixes': ('auc_house_name', 'auc_house_auth', 'auc_house_ulan')},
			}),
			GroupKeys(mapping={
				'lugt': {'properties': ('lugt_number_1', 'lugt_number_2', 'lugt_number_3')},
				'auc_copy': {'properties': ('auc_copy_seller_1', 'auc_copy_seller_2', 'auc_copy_seller_3', 'auc_copy_seller_4')},
				'other_seller': {'properties': ('other_seller_1', 'other_seller_2', 'other_seller_3')},
				'title_pg_sell': {'properties': ('title_pg_sell_1', 'title_pg_sell_2')},
				'location': {'properties': ('city_of_sale', 'sale_location', 'country_auth_1', 'country_auth_2', 'specific_loc')},
			}),
			add_auction_catalog,
			add_auction_event,
			populate_auction_event,
			add_uuid,
			AddDataDependentArchesModel(models=self.models),
# 			Trace(name='auction_events'),
			_input=records.output
		)
		if serialize:
			# write SALES data
			self.add_serialization_chain(graph, auction_events.output)
		return auction_events

	def add_buyers_sellers_chain(self, graph, acquisitions, serialize=True):
		for role in ('buyer', 'seller'):
			p = graph.add_chain(
				ExtractKeyedValues(key=role),
				AddDataDependentArchesModel(models=self.models),
				_input=acquisitions.output
			)
			if serialize:
				# write SALES data
				self.add_serialization_chain(graph, p.output)

	def add_acquisitions_chain(self, graph, sales, serialize=True):
		acqs = graph.add_chain(
			add_acquisition,
			AddDataDependentArchesModel(models=self.models),
			_input=sales.output
		)
		# TODO: add serialization of people for acq buyers and sellers
		if serialize:
			# write SALES data
			self.add_serialization_chain(graph, acqs.output)
		return acqs

	def add_sales_chain(self, graph, records, serialize=True):
		'''Add transformation of sales records to the bonobo pipeline.'''
		if self.limit is not None:
			records = graph.add_chain(
				Limit(self.limit),
				_input=records.output
			)
		sales = graph.add_chain(
			AddFieldNames(field_names=self.contents_headers),
			GroupRepeatingKeys(mapping={
				'expert': {'prefixes': ('expert_auth', 'expert_ulan')},
				'commissaire': {'prefixes': ('commissaire_pr', 'comm_ulan')},
				'auction_house': {'prefixes': ('auction_house', 'house_ulan')},
				'_artists': {'postprocess': add_record_ids, 'prefixes': ('artist_name', 'artist_info', 'art_authority', 'nationality', 'attrib_mod', 'attrib_mod_auth', 'star_rec_no', 'artist_ulan')},
				'hand_note': {'prefixes': ('hand_note', 'hand_note_so')},
				'seller': {'postprocess': lambda x, _: strip_key_prefix('sell_', x), 'prefixes': ('sell_name', 'sell_name_so', 'sell_name_ques', 'sell_mod', 'sell_auth_name', 'sell_auth_nameq', 'sell_auth_mod', 'sell_auth_mod_a', 'sell_ulan')},
				'price': {'postprocess': add_crom_price, 'prefixes': ('price_amount', 'price_currency', 'price_note', 'price_source', 'price_citation')},
				'buyer': {'postprocess': lambda x, _: strip_key_prefix('buy_', x), 'prefixes': ('buy_name', 'buy_name_so', 'buy_name_ques', 'buy_name_cite', 'buy_mod', 'buy_auth_name', 'buy_auth_nameQ', 'buy_auth_mod', 'buy_auth_mod_a', 'buy_ulan')},
				'prev_owner': {'prefixes': ('prev_owner', 'prev_own_ques', 'prev_own_so', 'prev_own_auth', 'prev_own_auth_D', 'prev_own_auth_L', 'prev_own_auth_Q', 'prev_own_ulan')},
				'prev': {'prefixes': ('prev_sale_year', 'prev_sale_mo', 'prev_sale_day', 'prev_sale_loc', 'prev_sale_lot', 'prev_sale_ques', 'prev_sale_artx', 'prev_sale_ttlx', 'prev_sale_note', 'prev_sale_coll', 'prev_sale_cat')},
				'post_sale': {'prefixes': ('post_sale_year', 'post_sale_mo', 'post_sale_day', 'post_sale_loc', 'post_sale_lot', 'post_sale_q', 'post_sale_art', 'post_sale_ttl', 'post_sale_nte', 'post_sale_col', 'post_sale_cat')},
				'post_owner': {'prefixes': ('post_own', 'post_own_q', 'post_own_so', 'post_own_auth', 'post_own_auth_D', 'post_own_auth_L', 'post_own_auth_Q', 'post_own_ulan')},
			}),
			GroupKeys(mapping={
				'auction_of_lot': {'properties': ('catalog_number', 'lot_number', 'lot_sale_year', 'lot_sale_month', 'lot_sale_day', 'lot_sale_mod', 'lot_notes')},
				'_object': {'postprocess': add_object_uuid, 'properties': ('title', 'title_modifier', 'object_type', 'materials', 'dimensions', 'formatted_dimens', 'format', 'genre', 'subject', 'inscription', 'present_loc_geog', 'present_loc_inst', 'present_loc_insq', 'present_loc_insi', 'present_loc_acc', 'present_loc_accq', 'present_loc_note', '_artists')},
				'bid': {'properties': ('est_price', 'est_price_curr', 'est_price_desc', 'est_price_so', 'start_price', 'start_price_curr', 'start_price_desc', 'start_price_so', 'ask_price', 'ask_price_curr', 'ask_price_so')},
			}),
# 			Trace(name='sale'),
			add_auction_of_lot,
			add_uuid,
			AddDataDependentArchesModel(models=self.models),
			# TODO: need to construct an LOD object for a sales record here so that it can be serialized")
			_input=records.output
		)
		if serialize:
			# write SALES data
			self.add_serialization_chain(graph, sales.output)
		return sales

	def add_object_chain(self, graph, sales, serialize=True):
		objects = graph.add_chain(
			ExtractKeyedValue(key='_object'),
			add_object_type,
			populate_object,
			MakeLinkedArtManMadeObject(),
			AddDataDependentArchesModel(models=self.models),
			add_pir_artists,
			_input=sales.output
		)
		if serialize:
			# write OBJECTS data
			self.add_serialization_chain(graph, objects.output)
		return objects

	def add_people_chain(self, graph, objects, serialize=True):
		'''Add transformation of artists records to the bonobo pipeline.'''
		model_id = self.models.get('Person', 'XXX-Person-Model')
		people = graph.add_chain(
			ExtractKeyedValues(key='_artists'),
			AddArchesModel(model=model_id),
			_input=objects.output
		)
		if serialize:
			# write PEOPLE data
			self.add_serialization_chain(graph, people.output)
		return people

	def _construct_graph(self):
		graph = bonobo.Graph()

		physical_catalog_records = graph.add_chain(
			MatchingFiles(path='/', pattern=self.catalogs_files_pattern, fs='fs.data.pir'),
			CurriedCSVReader(fs='fs.data.pir'),
		)

		auction_events_records = graph.add_chain(
			MatchingFiles(path='/', pattern=self.auction_events_files_pattern, fs='fs.data.pir'),
			CurriedCSVReader(fs='fs.data.pir'),
		)

		contents_records = graph.add_chain(
			MatchingFiles(path='/', pattern=self.contents_files_pattern, fs='fs.data.pir'),
			CurriedCSVReader(fs='fs.data.pir')
		)

		physical_catalogs = self.add_physical_catalogs_chain(graph, physical_catalog_records, serialize=True)
		auction_events = self.add_auction_events_chain(graph, auction_events_records, serialize=True)
		catalog_los = self.add_catalog_linguistic_objects(graph, auction_events, serialize=True)

		sales = self.add_sales_chain(graph, contents_records, serialize=True)
		objects = self.add_object_chain(graph, sales, serialize=True)
		acquisitions = self.add_acquisitions_chain(graph, objects, serialize=True)
		self.add_buyers_sellers_chain(graph, acquisitions, serialize=True)
		people = self.add_people_chain(graph, objects, serialize=True)

		self.graph = graph
		return graph

	def get_graph(self):
		'''Construct the bonobo pipeline to fully transform Provenance data from CSV to JSON-LD.'''
		if not self.graph:
			self._construct_graph()
		return self.graph

	def run(self, **options):
		'''Run the Provenance bonobo pipeline.'''
		sys.stderr.write("- Limiting to %d records per file\n" % (self.limit,))
		sys.stderr.write("- Using serializer: %r\n" % (self.serializer,))
		sys.stderr.write("- Using writer: %r\n" % (self.writer,))
		graph = self.get_graph(**options)
		services = self.get_services(**options)
		fs = services['fs.data.pir']
		bonobo.run(
			graph,
			services=services
		)

class ProvenanceFilePipeline(ProvenancePipeline):
	'''
	Provenance pipeline with serialization to files based on Arches model and resource UUID.

	If in `debug` mode, JSON serialization will use pretty-printing. Otherwise,
	serialization will be compact.
	'''
	def __init__(self, input_path, catalogs, auction_events, contents, **kwargs):
		super().__init__(input_path, catalogs, auction_events, contents, **kwargs)
		self.use_single_serializer = True
		self.output_chain = None
		debug = kwargs.get('debug', False)
		output_path = kwargs.get('output_path')

		if debug:
			self.serializer	= Serializer(compact=False)
			self.writer		= MergingFileWriter(directory=output_path)
			# self.writer	= MultiFileWriter(directory=output_path)
			# self.writer	= ArchesWriter()
		else:
			self.serializer	= Serializer(compact=True)
			self.writer		= MergingFileWriter(directory=output_path)
			# self.writer	= MultiFileWriter(directory=output_path)
			# self.writer	= ArchesWriter()


	def add_serialization_chain(self, graph, input_node):
		'''Add serialization of the passed transformer node to the bonobo graph.'''
		if self.use_single_serializer:
			if self.output_chain is None:
				self.output_chain = graph.add_chain(self.serializer, self.writer, _input=None)

			graph.add_chain(identity, _input=input_node, _output=self.output_chain.input)
		else:
			super().add_serialization_chain(graph, input_node)
