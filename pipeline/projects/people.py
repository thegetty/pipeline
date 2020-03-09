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
			strip_key_prefix
from pipeline.util.cleaners import parse_location_name
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


class PeopleUtilityHelper(UtilityHelper):
	'''
	Project-specific code for accessing and interpreting sales data.
	'''
	def __init__(self, project_name):
		super().__init__(project_name)
		self.csv_source_columns = ['star_record_no']
		self.person_identity = PersonIdentity(make_shared_uri=self.make_shared_uri, make_proj_uri=self.make_proj_uri)

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

class AddPerson(Configurable):
	helper = Option(required=True)
	
	def __call__(self, data:dict):
		if 'referred_to_by' not in data:
			data['referred_to_by'] = []

		name = data['auth_name']
		data['events'] = []
		data['places'] = []
		data['contact_point'] = []

		text_content = data.get('text')
		if text_content:
			cite = vocab.BiographyStatement(ident='', content=text_content)
			data['referred_to_by'].append(cite)

		source_content = data.get('source')
		if source_content:
			cite = vocab.BibliographyStatement(ident='', content=source_content)
			data['referred_to_by'].append(cite)

		locations = {l.strip() for l in data.get('location', '').split(';')} - {''}
		base_uri = self.helper.make_proj_uri('PLACE', '')
		for l in locations:
			current = parse_location_name(l, uri_base=self.helper.proj_prefix)
			place_data = self.helper.make_place(current, base_uri=base_uri)
			data['places'].append(place_data)

		addresses = {l.strip() for l in data.get('address', '').split(';')} - {''}
		for address in addresses:
			contact = vocab.Name(ident='', content=address)
			contact_data = add_crom_data(data={}, what=contact)
			data['contact_point'].append(contact_data)

		project = data.get('project')
		if project:
			data['referred_to_by'].append(vocab.SourceStatement(ident='', content=project))
		
		
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
	
			a = self.helper.person_identity.professional_activity(name, classified_as=types)
			data['events'].append(a)
			if self.helper.add_person(data):
				yield data


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
						'group': {
							'person': {
								'rename_keys': {
									'person_authority': 'auth_name',
									'ulan_id': 'ulan',
									'bith_date': 'birth',
									'death_date': 'death',
								},
								'properties': (
									'star_record_no',
									'person_authority',
									'variant_names',
									'type',
									'project',
									'birth_date',
									'death_date',
									'period_active',
									'century_active',
									'active_city_date',
									'nationality',
									'location',
									'address',
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
