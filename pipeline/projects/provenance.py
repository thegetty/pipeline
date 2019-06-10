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
from contextlib import suppress

import iso639
import lxml.etree
from sqlalchemy import create_engine
from langdetect import detect

import bonobo
from bonobo.config import Configurable, Option, use
from bonobo.constants import NOT_MODIFIED
from bonobo.nodes import Limit

import settings
from cromulent import model, vocab
from pipeline.util import identity, ExtractKeyedValue, ExtractKeyedValues, MatchingFiles
from pipeline.util.cleaners import date_cleaner
from pipeline.io.file import MultiFileWriter, MergingFileWriter
# from pipeline.io.arches import ArchesWriter
from pipeline.linkedart import \
			add_crom_data, \
			MakeLinkedArtLinguisticObject, \
			MakeLinkedArtOrganization, \
			MakeLinkedArtManMadeObject, \
			make_la_person
from pipeline.io.csv import CurriedCSVReader
from pipeline.nodes.basic import \
			add_uuid, \
			AddDataDependentArchesModel, \
			AddArchesModel, \
			Serializer, \
			Trace

legacyIdentifier = None # TODO: aat:LegacyIdentifier?
doiIdentifier = vocab.DoiIdentifier
variantTitleIdentifier = vocab.Identifier # TODO: aat for variant titles?

# utility functions

class MakePIRSalesDict(Configurable):
	headers = Option(list)
	def __call__(self, data):
		pairs = zip(self.headers, data)
# 		pairs = filter(lambda p: p[1] != '', pairs)
		yield dict(pairs)

class GroupRepeatingKeys(Configurable):
	mapping = Option(dict)
	drop_empty = Option(bool, default=True)
	def __call__(self, data):
		d = data.copy()
		for key, mapping in self.mapping.items():
			property_prefixes = mapping['prefixes']
			postprocess = mapping.get('postprocess')
			d[key] = []
			with suppress(KeyError):
				for i in itertools.count(1):
					ks = ((prefix, f'{prefix}_{i}') for prefix in property_prefixes)
					subd = {}
					for p, k in ks:
						subd[p] = d[k]
						del d[k]
					if self.drop_empty:
						values_unset = list(map(lambda v: not bool(v), subd.values()))
						if all(values_unset):
							continue
					if postprocess:
						subd = postprocess(subd, data)
					d[key].append(subd)
		yield d

class GroupKeys(Configurable):
	mapping = Option(dict)
	drop_empty = Option(bool, default=True)
	def __call__(self, data):
		d = data.copy()
		for key, mapping in self.mapping.items():
			subd = {}
			properties = mapping['properties']
			postprocess = mapping.get('postprocess')
			for k in properties:
				v = d[k]
				del d[k]
				if self.drop_empty and not v:
					continue
				subd[k] = v
			if postprocess:
				subd = postprocess(subd, data)
			d[key] = subd
		yield d

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

def add_crom_price(data, parent):
	amnt = model.MonetaryAmount()
	if data.get('price_amount'):
		v = data['price_amount']
		try:
			amnt.value = float(v)
		except ValueError:
			print(f'*** Not a numeric price amount: {v}')
			amnt._label = v # TODO: is there a way to associate the value string with the MonetaryAmount in a meaningful way?
	if data.get('price_currency'):
		print(f'*** CURRENCY: {data["price_currency"]}')
		currency = data["price_currency"]
		if currency in vocab.instances:
			amnt.currency = vocab.instances[currency]
		else:
			print(f'*** No currency instance defined for {currency}')
	add_crom_data(data=data, what=amnt)
	return data

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
	
	type = data['object_type'].lower()
	if type.lower() in TYPES:
		otype = TYPES[type]
		add_crom_data(data=data, what=otype())
	else:
		print(f'*** No object type for {type}')
		add_crom_data(data=data, what=model.ManMadeObject())
	return data

@use('uuid_cache')
def add_pir_artists(data, uuid_cache=None):
	lod_object = data['_LOD_OBJECT']
	event = model.Production()
	lod_object.produced_by = event

	artists = data.get('_artists', [])
	for i, a in enumerate(artists):
		star_rec_no = a.get('star_rec_no')
		a['uid'] = f'ARTIST-{star_rec_no}'
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

# Provenance Pipeline class

class ProvenancePipeline:
	'''Bonobo-based pipeline for transforming Provenance data from CSV into JSON-LD.'''
	def __init__(self, input_path, header_file,  files_pattern, **kwargs):
		self.graph = None
		self.models = kwargs.get('models', {})
		self.header_file = header_file
		self.files_pattern = files_pattern
		self.limit = kwargs.get('limit')
		self.debug = kwargs.get('debug', False)
		self.input_path = input_path

		fs = bonobo.open_fs(input_path)
		with fs.open(header_file, newline='') as csvfile:
			r = csv.reader(csvfile)
			self.headers = next(r)

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

	def add_sales_chain(self, graph, records, serialize=True):
		'''Add transformation of sales records to the bonobo pipeline.'''
		if self.limit is not None:
			records = graph.add_chain(
				Limit(self.limit),
				_input=records.output
			)
		sales = graph.add_chain(
			MakePIRSalesDict(headers=self.headers),
			GroupRepeatingKeys(mapping={
				'expert': {'prefixes': ('expert_auth', 'expert_ulan')},
				'commissaire': {'prefixes': ('commissaire_pr', 'comm_ulan')},
				'auction_house': {'prefixes': ('auction_house', 'house_ulan')},
				'_artists': {'postprocess': add_record_ids, 'prefixes': ('artist_name', 'artist_info', 'art_authority', 'nationality', 'attrib_mod', 'attrib_mod_auth', 'star_rec_no', 'artist_ulan')},
				'hand_note': {'prefixes': ('hand_note', 'hand_note_so')},
				'seller': {'prefixes': ('sell_name', 'sell_name_so', 'sell_name_ques', 'sell_mod', 'sell_auth_name', 'sell_auth_nameq', 'sell_auth_mod', 'sell_auth_mod_a', 'sell_ulan')},
				'price': {'postprocess': add_crom_price, 'prefixes': ('price_amount', 'price_currency', 'price_note', 'price_source', 'price_citation')},
				'buyer': {'prefixes': ('buy_name', 'buy_name_so', 'buy_name_ques', 'buy_name_cite', 'buy_mod', 'buy_auth_name', 'buy_auth_nameQ', 'buy_auth_mod', 'buy_auth_mod_a', 'buy_ulan')},
				'prev_owner': {'prefixes': ('prev_owner', 'prev_own_ques', 'prev_own_so', 'prev_own_auth', 'prev_own_auth_D', 'prev_own_auth_L', 'prev_own_auth_Q', 'prev_own_ulan')},
				'prev': {'prefixes': ('prev_sale_year', 'prev_sale_mo', 'prev_sale_day', 'prev_sale_loc', 'prev_sale_lot', 'prev_sale_ques', 'prev_sale_artx', 'prev_sale_ttlx', 'prev_sale_note', 'prev_sale_coll', 'prev_sale_cat')},
				'post_sale': {'prefixes': ('post_sale_year', 'post_sale_mo', 'post_sale_day', 'post_sale_loc', 'post_sale_lot', 'post_sale_q', 'post_sale_art', 'post_sale_ttl', 'post_sale_nte', 'post_sale_col', 'post_sale_cat')},
				'post_owner': {'prefixes': ('post_own', 'post_own_q', 'post_own_so', 'post_own_auth', 'post_own_auth_D', 'post_own_auth_L', 'post_own_auth_Q', 'post_own_ulan')},
			}),
			GroupKeys(mapping={
				'auction_of_lot': {'properties': ('catalog_number', 'lot_number', 'lot_sale_year', 'lot_sale_month', 'lot_sale_day', 'lot_sale_mod', 'lot_notes')},
				'object': {'postprocess': add_object_uuid, 'properties': ('title', 'title_modifier', 'object_type', 'materials', 'dimensions', 'formatted_dimens', 'format', 'genre', 'subject', 'inscription', 'present_loc_geog', 'present_loc_inst', 'present_loc_insq', 'present_loc_insi', 'present_loc_acc', 'present_loc_accq', 'present_loc_note', '_artists')},
				'bid': {'properties': ('est_price', 'est_price_curr', 'est_price_desc', 'est_price_so', 'start_price', 'start_price_curr', 'start_price_desc', 'start_price_so', 'ask_price', 'ask_price_curr', 'ask_price_so')},
			}),
# 			Trace(name='sale'),
			_input=records.output
		)
		if serialize:
			# write SALES data
			self.add_serialization_chain(graph, sales.output)
		return sales

	def add_object_chain(self, graph, sales, serialize=True):
		objects = graph.add_chain(
			ExtractKeyedValue(key='object'),
			add_object_type,
			MakeLinkedArtManMadeObject(),
			AddDataDependentArchesModel(models=self.models),
			add_pir_artists,
			_input=sales.output
		)
		if serialize:
			# write OBJECTS data
			self.add_serialization_chain(graph, objects.output)
		return objects

	def add_people_chain(self, graph, input, serialize=True):
		'''Add transformation of author records to the bonobo pipeline.'''
		model_id = self.models.get('Person', 'XXX-Person-Model')
		people = graph.add_chain(
			ExtractKeyedValues(key='_artists'),
			AddArchesModel(model=model_id),
			_input=input.output
		)
		if serialize:
			# write PEOPLE data
			self.add_serialization_chain(graph, people.output)
		return people

	def _construct_graph(self):
		graph = bonobo.Graph()
		records = graph.add_chain(
			MatchingFiles(path='/', pattern=self.files_pattern, fs='fs.data.pir'),
			CurriedCSVReader(fs='fs.data.pir')
		)
		
		sales = self.add_sales_chain(graph, records, serialize=False)
		objects = self.add_object_chain(graph, sales)
		people = self.add_people_chain(graph, objects)
		
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
	def __init__(self, input_path, header_file, files_pattern, **kwargs):
		super().__init__(input_path, header_file, files_pattern, **kwargs)
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
