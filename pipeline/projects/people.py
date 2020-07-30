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
from pipeline.projects import PipelineBase, UtilityHelper, PersonIdentity
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
			strip_key_prefix, \
			label_for_timespan_range, \
			timespan_from_outer_bounds
from pipeline.util.cleaners import date_parse, date_cleaner, parse_location_name
from pipeline.io.file import MergingFileWriter
from pipeline.io.memory import MergingMemoryWriter
import pipeline.linkedart
from pipeline.linkedart import add_crom_data, get_crom_object
from pipeline.io.csv import CurriedCSVReader
from pipeline.nodes.basic import \
			RemoveKeys, \
			KeyManagement, \
			GroupRepeatingKeys, \
			GroupKeys, \
			AddArchesModel, \
			Serializer, \
			OnlyRecordsOfType, \
			Trace
from pipeline.nodes.basic import AddFieldNamesSimple as AddFieldNames
from pipeline.util.rewriting import rewrite_output_files, JSONValueRewriter

#mark - utility functions and classes

class PeoplePersonIdentity(PersonIdentity):
	pass

class PeopleUtilityHelper(UtilityHelper):
	'''
	Project-specific code for accessing and interpreting sales data.
	'''
	def __init__(self, project_name):
		super().__init__(project_name)
		self.csv_source_columns = ['star_record_no']
		self.person_identity = PeoplePersonIdentity(make_shared_uri=self.make_shared_uri, make_proj_uri=self.make_proj_uri)

	def copy_source_information(self, dst: dict, src: dict):
		for k in self.csv_source_columns:
			with suppress(KeyError):
				dst[k] = src[k]
		return dst

	def add_person(self, data, **kwargs):
		auth_name = data.get('auth_name')
		names = data.get('variant_names')
		if self.person_identity.acceptable_person_auth_name(auth_name) or names:
			return super().add_person(data, **kwargs)
		else:
			return None

	def add_group(self, data, **kwargs):
		g = super().add_group(data, **kwargs)
		
		# "people" records that are actually groups are recorded here, serialized, and
		# then used in the Knoedler pipeline so that those records can be properly
		# asserted as Gorups and not People (since the distinguishing data only appears)
		# in the PEOPLE dataset, not in Knoedler.
		key = data['uri_keys']
		self.services['people_groups']['group_keys'].append(key)
		return g

class AddPerson(Configurable):
	helper = Option(required=True)

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.single_year = re.compile(r'[ ]?\(((in )?\d{4}[^)]*)\)')
		self.year_range = re.compile(r'[ ]?\((\d{4}-\d{4}[^)]*)\)')
		self.bound_year = re.compile(r'[ ]?\(((until|from|after) \d{4}[^)]*)\)')
		self.c_year = re.compile(r'[ ]?\((c \d{4}[^)]*)\)')
	
	def handle_dates(self, data):
		if 'birth' in data:
			data['birth_clean'] = date_cleaner(data['birth'])

		if 'death' in data:
			data['death_clean'] = date_cleaner(data['death'])

	def handle_statements(self, data):
		text_content = data.get('text')
		if text_content:
			cite = vocab.BiographyStatement(ident='', content=text_content)
			data['referred_to_by'].append(cite)

		source_content = data.get('source')
		if source_content:
			cite = vocab.BibliographyStatement(ident='', content=source_content)
			data['referred_to_by'].append(cite)

		project = data.get('project')
		if project:
			data['referred_to_by'].append(vocab.SourceStatement(ident='', content=project))

		awards = {l.strip() for l in data.get('medal_received', '').split(';')} - {''}
		for award in awards:
			cite = vocab.Note(ident='', content=award) # TODO: add proper classification for an Awards Statement
			data['referred_to_by'].append(cite)

	def handle_places(self, data):
		base_uri = self.helper.make_proj_uri('PLACE', '')
		for loc in data.get('locations', []):
			l = loc.get('location')
			if l:
				current = parse_location_name(l, uri_base=self.helper.proj_prefix)
				place_data = self.helper.make_place(current, base_uri=base_uri)
				data['places'].append(place_data)
				note = loc.get('location_note')
				if note:
					note = vocab.Note(ident='', content=note)
					data['referred_to_by'].append(note)
				date = loc.get('location_date')
				if date:
					note = vocab.BibliographyStatement(ident='', content=f'Residence in {l} ({date})')
					data['referred_to_by'].append(note)

			address = loc.get('address')
			if address:
				contact = model.Identifier(ident='', content=address)
				contact_data = add_crom_data(data={}, what=contact)
				data['contact_point'].append(contact_data)
				note = loc.get('address_note')
				if note:
					note = vocab.Note(ident='', content=note)
					data['referred_to_by'].append(note)
				date = loc.get('address_date')
				if date:
					note = vocab.BibliographyStatement(ident='', content=f'Address at {l} ({date})')
					data['referred_to_by'].append(note)

	def model_person_or_group(self, data):
		name = data['auth_name']
		type = {t.strip() for t in data.get('type', '').lower().split(';')} - {''}
		if type & {'institution', 'museum'}:
			# This is an Organization
			with suppress(KeyError):
				del data['nationality']
			if 'museum' in type:
				data['object_type'] = vocab.MuseumOrg
			if self.helper.add_group(data):
				yield data
		else:
			# This is a Person
			types = []
			if 'collector' in type:
				types.append(vocab.Collecting)
			if 'artist' in type:
				types.append(vocab.Creating)
			if 'dealer' in type:
				types.append(vocab.Dealing)
			if 'owner' in type:
				types.append(vocab.Owning)

			remaining = type - {'collector', 'artist', 'dealer', 'owner'}
			if remaining:
				warnings.warn(f'UNHANDLED PEOPLE TYPES: {remaining}')

			active_args = self.helper.person_identity.clamped_timespan_args(data, name)
			for t in types:
				a = self.helper.person_identity.professional_activity(name, classified_as=[t], **active_args)
				data['events'].append(a)

			for k in ('century_active', 'period_active'):
				with suppress(KeyError):
					del data[k]

			if self.helper.add_person(data):
				yield data

	def __call__(self, data:dict):
		star_id = data.get('star_record_no')
		data.setdefault('referred_to_by', [])
		data.setdefault('events', [])
		data.setdefault('places', [])
		data.setdefault('contact_point', [])
		data.setdefault('identifiers', [self.helper.gri_number_id(star_id)])

		self.handle_dates(data)
		self.handle_statements(data)
		self.handle_places(data)
		yield from self.model_person_or_group(data)

#mark - People Pipeline class

class PeoplePipeline(PipelineBase):
	'''Bonobo-based pipeline for transforming People data from CSV into JSON-LD.'''
	def __init__(self, input_path, contents, **kwargs):
		project_name = 'people'
		self.input_path = input_path
		self.services = None

		helper = PeopleUtilityHelper(project_name)

		super().__init__(project_name, helper=helper)

		self.graph = None
		self.models = kwargs.get('models', settings.arches_models)
		self.contents_header_file = contents['header_file']
		self.contents_files_pattern = contents['files_pattern']
		self.limit = kwargs.get('limit')
		self.debug = kwargs.get('debug', False)

		fs = bonobo.open_fs(input_path)
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
			'people_groups': {'group_keys': []},
		})
		return services

	def _construct_graph(self, services=None):
		'''
		Construct bonobo.Graph object for the entire pipeline.
		'''
		g = bonobo.Graph()

		contents_records = g.add_chain(
			MatchingFiles(path='/', pattern=self.contents_files_pattern, fs='fs.data.people'),
			CurriedCSVReader(fs='fs.data.people', limit=self.limit, field_names=self.contents_headers),
			KeyManagement(
				operations=[
					{
						'group_repeating': {
							'locations': {
								'prefixes': (
									'location',
									'location_date',
									'location_note',
									'address',
									'address_date',
									'address_note',
								)
							}
						}
					},
					{
						'group': {
							'person': {
								'rename_keys': {
									'person_authority': 'auth_name',
									'person_auth_disp': 'auth_display_name',
									'ulan_id': 'ulan',
									'birth_date': 'birth',
									'death_date': 'death',
									'notes': 'internal_notes'
								},
								'properties': (
									'star_record_no',
									'person_authority',
									'person_auth_disp',
									'variant_names',
									'type',
									'project',
									'birth_date',
									'death_date',
									'period_active',
									'century_active',
									'active_city_date',
									'nationality',
									'locations',
									'subjects_painted',
									'source',
									'medal_received',
									'text',
									'notes',
									'brief_notes',
									'working_notes',
									'bibliography',
									'ulan_id',
									'segment',
								)
							}
						}
					}
				]
			),
# 			Trace(name='foo', ordinals=range(10)),
			ExtractKeyedValue(key='person'),
			AddPerson(helper=self.helper),
		)

		_ = self.add_person_or_group_chain(g, contents_records, serialize=True)
		_ = self.add_places_chain(g, contents_records, key='places', serialize=True)
		

		self.graph = g

	def get_graph(self, **kwargs):
		'''Return a single bonobo.Graph object for the entire pipeline.'''
		if not self.graph:
			self._construct_graph(**kwargs)

		return self.graph

	def run(self, services=None, **options):
		'''Run the People bonobo pipeline.'''
		print(f'- Limiting to {self.limit} records per file', file=sys.stderr)
		if not services:
			services = self.get_services(**options)

		print('Running graph component...', file=sys.stderr)
		graph = self.get_graph(**options, services=services)
		self.run_graph(graph, services=services)

		print('Serializing static instances...', file=sys.stderr)
		for model, instances in self.static_instances.used_instances().items():
			g = bonobo.Graph()
			nodes = self.serializer_nodes_for_model(model=self.models[model], use_memory_writer=False)
			values = instances.values()
			source = g.add_chain(GraphListSource(values))
			self.add_serialization_chain(g, source.output, model=self.models[model], use_memory_writer=False)
			self.run_graph(g, services={})

		print('Writing people-groups mapping data to disk')
		pg_file = pathlib.Path(settings.pipeline_tmp_path).joinpath('people_groups.json')
		with pg_file.open('w') as fh:
			json.dump(services['people_groups'], fh)

class PeopleFilePipeline(PeoplePipeline):
	'''
	People pipeline with serialization to files based on Arches model and resource UUID.

	If in `debug` mode, JSON serialization will use pretty-printing. Otherwise,
	serialization will be compact.
	'''
	def __init__(self, input_path, contents, **kwargs):
		super().__init__(input_path, contents, **kwargs)
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

	def run(self, **options):
		'''Run the People bonobo pipeline.'''
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
