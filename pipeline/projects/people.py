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
from datetime import timedelta
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
			associate_with_tgn_record, \
			associate_with_tgn_record_people, \
			timespan_from_outer_bounds, \
			traverse_static_place_instances_people
from pipeline.util.cleaners import date_parse, date_cleaner, parse_location_name
from pipeline.io.file import MergingFileWriter
from pipeline.io.memory import MergingMemoryWriter
import pipeline.linkedart
from pipeline.linkedart import add_crom_data, get_crom_object, make_tgn_place, make_la_place
from pipeline.io.csv import CurriedCSVReader
from pipeline.nodes.basic import \
			RemoveKeys, \
			KeyManagement, \
			PreserveCSVFields, \
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
		g = super().add_group(data, split_notes=False, **kwargs)
		
		# "people" records that are actually groups are recorded here, serialized, and
		# then used in the Knoedler pipeline so that those records can be properly
		# asserted as Gorups and not People (since the distinguishing data only appears)
		# in the PEOPLE dataset, not in Knoedler.
		key = data['uri_keys']
		self.services['people_groups']['group_keys'].append(key)
		return g

class AddPersonEntry(Configurable):
	helper = Option(required=True)
	
	def __call__(self, data:dict):
		'''Add modeling for the entry describing a person/group in the PSCP PEOPLE dataset.'''
		recno = data['star_record_no']
		auth_name = data.get('auth_name')
		record_uri = self.helper.make_proj_uri('ENTRY', 'PEOPLE', recno)
		content = data['star_csv_data']
		record = vocab.EntryTextForm(ident=record_uri, label=f'Entry recorded in PSCP PEOPLE dataset for {auth_name}', content=content)
		creation = model.Creation(ident='')
		creation.carried_out_by = self.helper.static_instances.get_instance('Group', 'gpi')
		record.created_by = creation
		record.identified_by = self.helper.gpi_number_id(recno, vocab.StarNumber)
		record.identified_by = vocab.PrimaryName(ident='', content=f'STAR Person Authority Entry {recno}')
		record.part_of = self.helper.static_instances.get_instance('LinguisticObject', 'db-people')
		data['_entry_record'] = add_crom_data({'uri': record_uri}, record)

		yield data

class AddPerson(Configurable):
	helper = Option(required=True)

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.single_year = re.compile(r'[ ]?\(((in )?\d{4}[^)]*)\)')
		self.year_range = re.compile(r'[ ]?\((\d{4}-\d{4}[^)]*)\)')
		self.bound_year = re.compile(r'[ ]?\(((until|from|after) \d{4}[^)]*)\)')
		self.c_year = re.compile(r'[ ]?\((c \d{4}[^)]*)\)')
		self.active_date = re.compile(r'(.*?)[ ]+\(([^)]+)\)')
	
	def clean_dates(self, data):
		cb = data.get('corporate_body', False)
		birth_key = 'formation' if cb else 'birth'
		death_key = 'dissolution' if cb else 'death'

		if cb:
			data[birth_key] = data.get('birth')
			data[death_key] = data.get('death')

		if 'birth' in data:
			data[f'{birth_key}_clean'] = date_cleaner(data[birth_key])

		if 'death' in data:
			if data[death_key] == 10000:
				with open('date_10k.txt', 'a') as f:
					f.write(str(data.get('star_record_no')))
					f.write('\n')
			data[f'{death_key}_clean'] = date_cleaner(data[death_key])
		
		birth = data.get('birth')
		death = data.get('death')
		period = data.get('period_active')
		century = data.get('century_active')
		active_city = data.get('active_city_date')
		
		if century:
			if '-' in century:
				begin, end = century.split('-')
			else:
				begin, end = century, century
			begin_ts = date_cleaner(begin)
			end_ts = date_cleaner(end)
			range_ts = [None, None]
			with suppress(TypeError):
				range_ts[0] = begin_ts[0]
			with suppress(TypeError):
				range_ts[1] = end_ts[1]
			data['century_active_clean'] = range_ts
		
		if period:
			clean_ts = []
			components = [p.strip() for p in period.split(';')]
			ts = None
			for p in components:
				if '-' in p:
					begin, end = p.split('-')
					begin_ts = date_cleaner(begin)
					end_ts = date_cleaner(end)
					ts = [None, None]
					with suppress(TypeError):
						ts[0] = begin_ts[0]
					with suppress(TypeError):
						ts[1] = end_ts[1]
				else:
					ts = date_cleaner(p)
				if ts:
					clean_ts.append(ts)
			if ts:
				# TODO: should handle multiple timespans in clean_ts
				data['period_active_clean'] = ts

	def handle_statements(self, data):
		record = get_crom_object(data['_entry_record'])
		data['referred_to_by'].append(record)

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


	def new_residence_activity(self, place, group, record):
		res_act = model.Activity(ident=self.helper.make_proj_uri('Activity',  'establishment', group.id, place.id))
		res_act.took_place_at = place
		res_type = model.Type(ident='http://vocab.getty.edu/aat/300393212', label="Establishment")
		location_type = model.Type(ident='http://vocab.getty.edu/aat/300393211', label="Location Activity or State")
		res_type.classified_as = location_type
		res_act.classified_as = res_type
		res_act.referred_to_by = record

		return res_act

	def model_active_city(self, data, loc):
		m = self.active_date.search(loc)
		date = None
		if m:
			date = m.group(2)
			if not any(char.isdigit() for char in date):
				date = None
				date_range = None
			else:
				date_range = date_cleaner(date)
			
			if date_range:
				loc = m.group(1)
			else:
				# this apparently wasn't a date, but just a parenthetical
				date = None
		# handle Brazil problem
		if loc == 'Brazil':
			loc = 'Brasil'

		if 'loc_tgn' in data:

			if len(data['loc_tgn']) > 1 and loc in data['loc_tgn']:			
				sdata = {
					'location': loc,
					'loc_tgn': data['loc_tgn'][loc]
				}

			elif len(data['loc_tgn']) > 1 and loc not in data['loc_tgn']:			
				sdata = {
					'location': loc,
				}

			elif len(data['loc_tgn']) == 1 and loc in data['loc_tgn']:
				
				sdata = {
					'location': loc,
					'loc_tgn': data['loc_tgn'][loc]
				}
			
			elif len(data['loc_tgn']) == 1 and loc not in data['loc_tgn']:
				if any(isinstance(i,dict) for i in data['loc_tgn'].values()):
					sdata = {
						'location': loc,
					}
				else:
					sdata = {
					'location': loc,
					'loc_tgn': data['loc_tgn']
					}


			else: # check here it may is wrong
				sdata = {
					'location': loc,
			}
		
		else: 
			sdata = {
				'location': loc,
			}

		if date:
			sdata['address_date'] = date		
		return self.model_sojourn(data, sdata)

	def model_sojourn(self, data, loc):

		base_uri = self.helper.make_proj_uri('PLACE', '')
		cb = data.get('corporate_body', False)
		sojourn_type = vocab.Establishment if cb else vocab.Residing
		if not 'classified_as' in sojourn_type.__dict__['_classification'][0].__dict__:
			print('hi')

		sdata = {
			'type': sojourn_type,
			'referred_to_by': [],
		}
		 
		verbatim_date = loc.get('address_date')
		if verbatim_date:
			date_range = date_cleaner(verbatim_date)
			if date_range:
				begin, end = date_range
				ts = timespan_from_outer_bounds(*date_range)
				ts.identified_by = model.Name(ident='', content=verbatim_date)
				sdata['timespan'] = add_crom_data({'address_date': verbatim_date, 'begin': begin, 'end': end}, ts)
		
		current = None
		l = loc.get('location')
		tgn_data = loc.get('loc_tgn')

		if l:
			current = parse_location_name(l, uri_base=self.helper.proj_prefix)
		address = loc.get('address')
		if address:
			current = {
				'name': address + ', ' + l,
				# 'part_of': current,
				'type': 'address',
			}
		for k in ('address_note', 'location_note'):
			note = loc.get(k)
			if note:
				sdata['referred_to_by'].append(vocab.Note(ident='', content=note))
		### HERE
		if current:

			if not tgn_data:

				place_data = self.helper.make_place(current, base_uri=base_uri)
				# import pdb; pdb.set_trace()
				data['_places'].append(place_data)
				sdata['place'] = place_data
				# with open('log_no_tgn.txt', 'a') as f:
				# 	f.write(data['star_record_no'])
				# 	f.write(' ')
				# 	f.write(data['parent_data']['person']['active_city_date'])
				# 	f.write('\n\n')

			else:
			# if tgn_data:
				# print("star_record_no: ", data['star_record_no'])
				part_of = tgn_data.get("part_of") # this is a tgn id
				same_as = tgn_data.get('same_as') # this is a tgn id
				if l in tgn_data:
					part_of = tgn_data[l].get("part_of") # this is a tgn id
					same_as = tgn_data[l].get('same_as') # this is a tgn id

				if part_of:
					tgn_instance = self.helper.static_instances.get_instance('Place', part_of)
					traverse_static_place_instances_people(self, tgn_instance)
					place_data = self.helper.make_place(current, base_uri=base_uri)

					# place_data = make_la_place(
					# {
					# 	'name': current,
					# 	'uri': self.helper.make_shared_uri(('PLACE',l))
					# },
					# )
					o_place = get_crom_object(place_data)
					o_place.part_of = tgn_instance
					if len(o_place.part_of)>1:
						i = 0
						for part in o_place.part_of:
							if 'exact_match' not in part.__dict__:
								o_place.part_of.pop(i)
							i+=1

					data['_places'].append(place_data)
					sdata['place'] = place_data


				if same_as:
					tgn_instance = self.helper.static_instances.get_instance('Place', same_as)
					traverse_static_place_instances_people(self, tgn_instance)

					alternate_exists=False
					for id in tgn_instance.identified_by:
						if isinstance(id, vocab.AlternateName) and id.content == l:
							alternate_exists = True
						
						if not alternate_exists:
							tgn_instance.identified_by = vocab.AlternateName(ident=self.helper.make_shared_uri(('PLACE',current)), content=l)
					
					owner_place = tgn_instance

					sdata['tgn'] = tgn_instance

		return sdata

	def handle_places(self, data):
		data.setdefault('sojourns', [])
		hmo = get_crom_object(data)
		for i, loc in enumerate(data.get('locations', [])):
			sdata = self.model_sojourn(data, loc)
			data['sojourns'].append(sdata)

		active_cities = {t.strip() for t in data.get('active_city_date', '').split(';')} - {''}
		for i, loc in enumerate(sorted(active_cities)):
			sdata = self.model_active_city(data, loc)
			if sdata:
				sdata['active_city'] = 'yes'
				data['sojourns'].append(sdata)
				owner_place = None


	def model_person_or_group(self, data):
		name = data['auth_name']
		generic_name = data.get('generic_name')
		type = {t.strip() for t in data.get('type', '').lower().split(';')} - {''}

		data.setdefault('occupation', [])
		data.setdefault('object_type', [])
		if 'object_type' in data and not isinstance(data['object_type'], list):
			data['object_type'] = [data['object_type']]

		cb = data.get('corporate_body')
		active_args = self.helper.person_identity.clamped_timespan_args(data, name)
		if cb:
			# This is an Organization
			with suppress(KeyError):
				del data['nationality']
			if 'museum' in type:
				data['object_type'].append(vocab.MuseumOrg)
			if 'institution' in type:
				data['object_type'].append(vocab.Institution)
			# if active_args:
			a = self.helper.person_identity.professional_activity(name, classified_as=[vocab.ActiveOccupation], **active_args)
			data['events'].append(a)
				
			if self.helper.add_group(data):
				yield data
		else:
			# This is a Person
			types = []
			if 'collector' in type:
				types.append(vocab.CollectingOccupation)
				data['occupation'].append(vocab.instances.get('collector occupation'))
			if 'artist' in type:
				types.append(vocab.CreatingOccupation)
				data['occupation'].append(vocab.instances.get('artist occupation'))
			if 'dealer' in type:
				types.append(vocab.DealingOccupation)
				data['occupation'].append(vocab.instances.get('dealer occupation'))
			if 'owner' in type:
				types.append(vocab.OwningOccupation)

			remaining = type - {'collector', 'artist', 'dealer', 'owner'}
			if remaining:
				warnings.warn(f'UNHANDLED PEOPLE TYPES: {remaining}')

			# professional_activity calls active_timespan which uses inclusive dates.
			# but active_args are parsed by date_cleaner which returns exclusive dates.
			# So we need to remove the final day on the end_of_end date
			if 'date_range' in active_args and len(active_args['date_range']) == 2 and active_args['date_range'][1] is not None:
				start, end = active_args['date_range']
				end -= timedelta(days=1)
				active_args['date_range'] = (start, end)

			# model professional activity, but not if this record is a generic group.
			if not self.helper.person_identity.is_anonymous_group(generic_name):
				for t in types:
					a = self.helper.person_identity.professional_activity(name, classified_as=[t], **active_args)
					data['events'].append(a)

			if self.helper.add_person(data):
				yield data

	def __call__(self, data:dict):
		star_id = data.get('star_record_no')
		data.setdefault('referred_to_by', [])
		data.setdefault('events', [])
		data.setdefault('_places', [])
		data.setdefault('identifiers', [self.helper.gpi_number_id(star_id)])

		self.clean_dates(data)
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

		vocab.register_instance('address', {'parent': model.Type, 'id': '300386983', 'label': 'Street Address'})
		vocab.register_instance('location', {'parent': model.Type, 'id': '300393211', 'label': 'Location'})
		vocab.register_instance('occupation', {'parent': model.Type, 'id': '300263369', 'label': 'Occupation'})

		vocab.register_vocab_class('Residing', {"parent": model.Activity, "id":"300393179", "label": "Residing", "metatype": "location"})
		vocab.register_vocab_class('Establishment', {"parent": model.Activity, "id":"300393212", "label": "Establishment", "metatype": "location"})
		vocab.register_vocab_class('StreetAddress', {"parent": model.Identifier, "id":"300386983", "label": "Street Address"})

		vocab.register_vocab_class("CreatingOccupation", {"parent": model.Activity, "id":"300404387", "label": "Creating Artwork", "metatype": "occupation"})
		vocab.register_vocab_class("CollectingOccupation", {"parent": model.Activity, "id":"300077121", "label": "Collecting", "metatype": "occupation"})
		vocab.register_vocab_class("DealingOccupation", {"parent": model.Activity, "id":"300055675", "label": "Commercial Dealing in Artwork", "metatype": "occupation"})
		vocab.register_vocab_class("OwningOccupation", {"parent": model.Activity, "id":"300055603", "label": "Owning", "metatype": "occupation"})

		vocab.register_instance('form type', {'parent': model.Type, 'id': '300444970', 'label': 'Form'})
		vocab.register_vocab_class('EntryTextForm', {"parent": model.LinguisticObject, "id":"300438434", "label": "Entry", "metatype": "form type"})
		
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

		# lookup dictionary with all authority information retrieved
		tgn_places = services.get('tgn', {}) 
		
		# lookup dictionary that maps knoedler a field and its value to a place 
		# either as same as or as falling within a place in tgn_places dict
		people_tgn = services.get('people_tgn', {}) 
		
		services['tgn'] = tgn_places
		services['people_tgn'] = people_tgn

		services.update({
			# to avoid constructing new MakeLinkedArtPerson objects millions of times, this
			# is passed around as a service to the functions and classes that require it.
			'make_la_person': pipeline.linkedart.MakeLinkedArtPerson(),
			'people_groups': {'group_keys': []},
		})
		return services

	def _static_place_instances(self):
		'''
		Create static instances for every place mentioned in the tgn service data.
		'''
		super()._static_place_instances()
		tgn_places = self.services['tgn']
		instances = {}
		places = {}
		
		for tgn_id, tgn_data in tgn_places.items():
			places[tgn_id] = tgn_data
		
		start = timeit.default_timer()	
		print("Started the tranformation of Static Places Instances...")
		for tgn_id, tgn_data in places.items():
			tgn_id = tgn_data.get('tgn_id')		
					
			place = make_tgn_place(tgn_data, self.helper.make_shared_uri, tgn_places)
			instances[tgn_id] = place
		print(f"Completed in {timeit.default_timer() - start}")
		return instances


	def _construct_graph(self, services=None):
		'''
		Construct bonobo.Graph object for the entire pipeline.
		'''
		g = bonobo.Graph()

		contents_records = g.add_chain(
			MatchingFiles(path='/', pattern=self.contents_files_pattern, fs='fs.data.people'),
			CurriedCSVReader(fs='fs.data.people', limit=self.limit, field_names=self.contents_headers),
			PreserveCSVFields(key='star_csv_data', order=self.contents_headers),
			KeyManagement(
				operations=[
					{
						'group_repeating': {
							'locations': {
								'postprocess': [
									lambda d, p: associate_with_tgn_record_people(d, p, services['people_tgn'],"address")
								],

								'prefixes': (
									'location',
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
								'postprocess': [
									lambda d, p: associate_with_tgn_record_people(d, p, services['people_tgn'],"active_city_date"),
								# 	lambda d, p: associate_with_tgn_record_people(d, p, services['people_tgn'],"location"),
								# 	lambda d, p: associate_with_tgn_record_people(d, p, services['people_tgn'],"address")
								],

								'rename_keys': {
									'person_authority': 'auth_name',
									'person_auth_disp': 'auth_display_name',
									'ulan_id': 'ulan',
									'birth_date': 'birth',
									'death_date': 'death',
									'notes': 'internal_notes'
								},
								'properties': (
									'star_csv_data',
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
									'corporate_body',
									'generic_name'
								)
							}
						}
					}
				]
			),
# 			Trace(name='foo', ordinals=range(10)),
			PreserveCSVFields(key='star_csv_data', order=self.contents_headers),
			ExtractKeyedValue(key='person'),
			AddPersonEntry(helper=self.helper),
			AddPerson(helper=self.helper),
		)

		_ = self.add_person_or_group_chain(g, contents_records, serialize=True)
		_ = self.add_places_chain(g, contents_records, key='_places', serialize=True)
		
		records = g.add_chain(
			ExtractKeyedValue(key='_entry_record'),
			_input=contents_records.output
		)
		
		self.add_serialization_chain(g, records.output, model=self.models['LinguisticObject'], use_memory_writer=False)

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

		print('Writing people-groups mapping data to disk', file=sys.stderr)
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
