import json
import timeit
import sys, os
import itertools
import pathlib
from sqlalchemy import create_engine
import bonobo
import bonobo_sqlalchemy

PROJECT_NAME = "knoedler"
UID_TAG_PREFIX = f'tag:getty.edu,2019:digital:pipeline:{PROJECT_NAME}:REPLACE-WITH-UUID#'

from pipeline.nodes.basic import AddArchesModel, AddFieldNamesService, Serializer, deep_copy, Offset, Trace
from pipeline.projects.knoedler.data import *
from pipeline.projects.knoedler.linkedart import *
from pipeline.io.file import FileWriter
from pipeline.io.arches import ArchesWriter
from pipeline.linkedart import make_la_person
from settings import *
import settings

class Pipeline:
	'''Bonobo-based pipeline for transforming Knoedler data into JSON-LD.'''
	def __init__(self, output_path, **kwargs):
		self.output_chain = None
		self.graph = None
		self.models = kwargs.get('models', settings.arches_models)
		self.pack_size = kwargs.get('pack_size')
		self.limit = kwargs.get('limit')
		self.debug = kwargs.get('debug', False)
		self.pipeline_service_files_path = kwargs.get('pipeline_service_files_path', settings.pipeline_service_files_path)

		if self.debug:
			self.SRLZ = Serializer(compact=False)
			self.WRITER = FileWriter(directory=output_path)
			# self.WRITER	= ArchesWriter()
			sys.stderr.write("In self.debugGING mode\n")
		else:
			self.SRLZ = Serializer(compact=True)
			self.WRITER = FileWriter(directory=output_path)
			# self.WRITER	= ArchesWriter()

	# Set up environment
	def get_services(self):
		'''Return a `dict` of named services available to the bonobo pipeline.'''
		services = {
			'trace_counter': itertools.count(),
			'gpi': create_engine(gpi_engine),
			'raw': create_engine(raw_engine)
		}
		
		p = pathlib.Path(self.pipeline_service_files_path)
		for file in p.rglob('*.json'):
			with open(file, 'r') as f:
				services[file.stem] = json.load(f)
		return services

	def add_sales(self, graph):
		graph.add_chain(
			bonobo_sqlalchemy.Select('SELECT * from knoedler_purchase_info', engine='gpi', limit=self.limit, pack_size=self.pack_size),
			AddFieldNamesService(key="purchase_info"),
			AddArchesModel(model=arches_models['Acquisition']),
			add_purchase_people,
			add_purchase_thing,
			add_ownership_phase_purchase,
			make_la_purchase,
			self.SRLZ,
			self.WRITER
		)

		phases = graph.add_chain(
			fan_object_phases,
			AddArchesModel(model=arches_models['Phase']),
			make_la_phase,
			self.SRLZ,
			self.WRITER,
			_input=add_ownership_phase_purchase
		)

		if self.debug and SPAM:
			graph.add_chain(print_jsonld, _input=phases.output)

		acqs = graph.add_chain(
			bonobo_sqlalchemy.Select('SELECT * from knoedler_sale_info', engine='gpi', limit=self.limit, pack_size=self.pack_size),
			AddFieldNamesService(key="sale_info"),
			AddArchesModel(model=arches_models['Acquisition']),
			add_sale_people,
			add_sale_thing, # includes adding reference to phase it terminates
			make_la_sale,
			self.SRLZ,
			self.WRITER
		)

		if self.debug and SPAM:
			graph.add_chain(print_jsonld, _input=acqs.output)

	def add_missing(self, graph):
		graph.add_chain(
			bonobo_sqlalchemy.Select('''
				SELECT pi_record_no, object_id, inventory_event_id, sale_event_id, purchase_event_id
				FROM knoedler
				WHERE inventory_event_id NOT NULL
				''',
				engine='gpi', limit=self.limit, pack_size=self.pack_size),
			find_raw,
			AddFieldNamesService(key="raw"),
			# bonobo.PrettyPrinter(),
			make_missing_purchase_data,
			make_missing_shared
		)

		graph.add_chain(
			make_missing_purchase,
			AddArchesModel(model=arches_models['Acquisition']),
			#bonobo.PrettyPrinter(),
			make_la_purchase,
			self.SRLZ,
			self.WRITER,
			_input=make_missing_shared
		)

		# This actually makes /all/ the inventory activities
		graph.add_chain(
			make_inventory,
			AddArchesModel(model=arches_models['Activity']),
			make_la_inventory,
			self.SRLZ,
			# bonobo.PrettyPrinter(),
			self.WRITER,
			_input=make_missing_shared
		)

	def add_pre_post(self, graph):
		chain1 = graph.add_chain(
			bonobo_sqlalchemy.Select('''
				SELECT pp.rowid, pp.previous_owner_uid, pp.object_id, p.person_ulan, p.person_label
				FROM knoedler_previous_owners AS pp
					JOIN gpi_people as p ON (p.person_uid = pp.previous_owner_uid)
				''', engine='gpi', limit=self.limit, pack_size=self.pack_size),
				AddFieldNamesService(key="prev_post_owners"),
				add_prev_prev
		)

		chain2 = graph.add_chain(
			bonobo_sqlalchemy.Select('''
				SELECT pp.rowid, pp.post_owner_uid, pp.object_id, p.person_ulan, p.person_label
				FROM
					knoedler_post_owners AS pp
					JOIN gpi_people as p ON (p.person_uid = pp.post_owner_uid)
				''',
				engine='gpi', limit=self.limit, pack_size=self.pack_size),
				AddFieldNamesService(key="prev_post_owners"),
		)

		for cin in [chain1.output, chain2.output]:
			graph.add_chain(
				AddArchesModel(model=arches_models['Acquisition']),
				fan_prev_post_purchase_sale,
				make_la_prev_post,
				self.SRLZ,
				self.WRITER,
				_input = cin
			)

	def add_objects(self, graph):
		graph.add_chain(
			bonobo_sqlalchemy.Select('SELECT DISTINCT object_id FROM knoedler', engine='gpi', limit=self.limit, pack_size=self.pack_size),
			make_objects,
			AddArchesModel(model=arches_models['HumanMadeObject']),
			make_objects_names,
			make_objects_dims,
			make_objects_tags_ids,
			make_objects_artists,
			make_la_object,
			self.SRLZ,
			self.WRITER
		)

		visitems = graph.add_chain(
			deep_copy,
			AddArchesModel(model=arches_models['VisualItem']),
			make_la_vizitem,
			self.SRLZ,
			self.WRITER,
			_input = make_objects_artists
		)

		if self.debug and SPAM:
			graph.add_chain(print_jsonld, _input=visitems.output)

	def add_people(self, graph):
		people = graph.add_chain(
			bonobo_sqlalchemy.Select('''
				SELECT DISTINCT peeps.*
				FROM
					gpi_people AS peeps
					JOIN gpi_people_names AS names ON (peeps.person_uid = names.person_uid)
					JOIN gpi_people_names_references AS ref ON (names.person_name_id = ref.person_name_id)
				WHERE
					ref.source_record_id LIKE "KNO%"
				''',
				engine='gpi', limit=self.limit, pack_size=self.pack_size),
			AddFieldNamesService(key="gpi_people"),
			AddArchesModel(model=arches_models['Person']),
			add_person_names,
			add_person_aat_labels,
			clean_dates,
			make_la_person,	
			self.SRLZ,
			self.WRITER
		)

		if self.debug and SPAM:
			graph.add_chain(print_jsonld, _input=people.output)

	def add_documents(self, graph):
		graph.add_chain(
			bonobo_sqlalchemy.Select('SELECT DISTINCT stock_book_no FROM knoedler ORDER BY stock_book_no', engine='gpi', limit=self.limit, pack_size=self.pack_size),
			make_stock_books,
			AddArchesModel(model=arches_models['LinguisticObject']),
			make_la_book,

			fan_pages,
			AddArchesModel(model=arches_models['LinguisticObject']),
			make_la_page,

			fan_rows,
			AddArchesModel(model=arches_models['LinguisticObject']),
			make_la_row
		)

		# create subsequent branches
		for xin in [make_la_book, make_la_page, make_la_row]:
			out = graph.add_chain(
				self.SRLZ,
				self.WRITER,
				_input = xin
			)
			if self.debug and SPAM:
				graph.add_chain(print_jsonld, _input=out.output)

	def get_graph(self):
		graph = bonobo.Graph()

		# Sales
		if not self.debug or 1:
			self.add_sales(graph)

		# Here we do both missing purchases and inventory events
		if not self.debug or 1:
			self.add_missing(graph)

		# Pre/Post owners
		if not self.debug or 1:
			self.add_pre_post(graph)

		# Objects
		if not self.debug or 1:
			self.add_objects(graph)

		# People
		if not self.debug or 1:
			self.add_people(graph)

		# Documents
		if not self.debug or 0:
			self.add_documents(graph)

		return graph

	def run(self, services=None, **options):
		'''Run the Knoedler bonobo pipeline.'''
		sys.stderr.write("- Limiting to %d records per file\n" % (self.limit,))
		sys.stderr.write("- Using serializer: %r\n" % (self.SRLZ,))
		sys.stderr.write("- Using writer: %r\n" % (self.WRITER,))
		if not services:
			services = self.get_services(**options)

		start = timeit.default_timer()
		graph = self.get_graph(**options)
		bonobo.run(graph, services=services)
		
		print('Pipeline runtime: ', timeit.default_timer() - start)  

