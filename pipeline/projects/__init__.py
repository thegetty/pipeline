import re
import sys
import pathlib
import pprint
import itertools
import json
import warnings
from collections import defaultdict
from contextlib import suppress

import urllib.parse
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

import bonobo
import settings

import pipeline.execution
from cromulent import model, vocab
from pipeline.util import \
			CaseFoldingSet, \
			ExtractKeyedValues, \
			timespan_for_century, \
			timespan_from_outer_bounds, \
			make_ordinal
from pipeline.util.cleaners import date_cleaner
from pipeline.linkedart import add_crom_data, get_crom_object
from pipeline.nodes.basic import \
			OnlyRecordsOfType, \
			AddArchesModel, \
			Serializer

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
		elif auth_name and self.is_anonymous_group(auth_name):
			key = ('GROUP', 'AUTH', auth_name)
			return key, self.make_shared_uri
		elif auth_name and self.acceptable_person_auth_name(auth_name):
			key = ('PERSON', 'AUTH', auth_name)
			return key, self.make_shared_uri
		else:
			# not enough information to identify this person uniquely, so use the source location in the input file
			if record_id:
				pi_rec_no = data['pi_record_no']
				key = ('PERSON', 'PI', pi_rec_no, record_id)
				return key, self.make_proj_uri
			elif 'pi_record_no' in data:
				pi_rec_no = data['pi_record_no']
				warnings.warn(f'*** No record identifier given for person identified only by pi_record_number {pi_rec_no}')
				key = ('PERSON', 'PI', pi_rec_no)
				return key, self.make_proj_uri
			else:
				star_record_no = data['star_record_no']
				warnings.warn(f'*** No record identifier given for person identified only by star_record_no {star_record_no}')
				key = ('PERSON', 'STAR', star_record_no)
				return key, self.make_proj_uri
				

	def add_person(self, a, record=None, relative_id=None, **kwargs):
		self.add_uri(a, record_id=relative_id)
		self.add_names(a, referrer=record, **kwargs)
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
		
	def professional_activity(self, name, century=None, date_range=None, **kwargs):
		a = vocab.Active(ident='', label=f'Professional activity of {name}')
		if century:
			ts = timespan_for_century(century, **kwargs)
			a.timespan = ts
		elif date_range:
			b, e = date_range
			ts = timespan_from_outer_bounds(begin=b, end=e, inclusive=True)
			a.timespan = ts
		return a

	def add_props(self, data:dict, role=None, **kwargs):
		if 'events' not in data:
			data['events'] = []
		role = role if role else 'person'
		auth_name = data.get('auth_name', '')
		period_match = self.anon_period_re.match(auth_name)
		nationalities = []
		if 'nationality' in data:
			nationality = data['nationality']
			if isinstance(nationality, str):
				nationalities.append(nationality.lower())
			elif isinstance(nationality, list):
				nationalities += [n.lower() for n in nationality]
		data['nationality'] = []
		
		period_active = data.get('period_active')
		century_active = data.get('century_active')
		name = data['label']
		if period_active:
			date_range = date_cleaner(period_active)
			if date_range:
				a = self.professional_activity(name, date_range=date_range)
				data['events'].append(a)
		elif century_active:
			if len(century_active) == 4 and century_active.endswith('th'):
				century = int(century_active[0:2])
				a = self.professional_activity(name, century=century)
				data['events'].append(a)
			else:
				warnings.warn(f'TODO: better handling for century ranges: {century_active}')

		if 'referred_to_by' not in data:
			data['referred_to_by'] = []

		for key in ('notes', 'brief_notes', 'working_notes'):
			if key in data:
				for content in [n.strip() for n in data[key].split(';')]:
					note = vocab.Note(ident='', content=content)
					data['referred_to_by'].append(note)

		for key in ('name_cite', 'bibliography'):
			if data.get(key):
				cite = vocab.BibliographyStatement(ident='', content=data[key])
				data['referred_to_by'].append(cite)

		if data.get('name_cite'):
			cite = vocab.BibliographyStatement(ident='', content=data['name_cite'])
			data['referred_to_by'].append(cite)

		if self.is_anonymous_group(auth_name):
			nationality_match = self.anon_nationality_re.match(auth_name)
			dated_nationality_match = self.anon_dated_nationality_re.match(auth_name)
			dated_match = self.anon_dated_re.match(auth_name)
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
					a = self.professional_activity(group_label, century=century, narrow=True)
					data['events'].append(a)
			elif dated_match:
				with suppress(ValueError):
					century = int(dated_match.group(1))
					group_label = self.anonymous_group_label(role, century=century)
					data['label'] = group_label
					a = self.professional_activity(group_label, century=century, narrow=True)
					data['events'].append(a)
			elif period_match:
				period = period_match.group(1).lower()
				data['label'] = f'anonymous {period} {role}s'
		for nationality in nationalities:
			key = f'{nationality.lower()} nationality'
			n = vocab.instances.get(key)
			if n:
				data['nationality'].append(n)
			else:
				warnings.warn(f'No nationality instance found in crom for: {key!r}')

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

		if 'names' not in data:
			data['names'] = []

		names = []
		name = data.get('name')
		if name:
			names.append(name)
		variant_names = data.get('variant_names')
		if variant_names:
			names += variant_names.split(';')

		for name in names:
			if role and not role_label:
				role_label = f'{role} “{name}”'
			if referrer:
				data['names'].append((name, {'referred_to_by': [referrer]}))
			else:
				data['names'].append(name)
			if 'label' not in data:
				data['label'] = name

		if 'label' not in data:
			data['label'] = '(Anonymous)'

		if role and not role_label:
			role_label = f'anonymous {role}'

		if role:
			data['role_label'] = role_label

class StaticInstanceHolder:
	'''
	This class wraps a dict that holds a set of crom objects, categorized by model name.
	
	Access to those objects is recorded, and at the end of a pipeline run, just those
	objects that were accessed can be returned (to be serialized). This helps to avoid
	serializing objects that are not relevant to a specific pipeline run (e.g. defined
	for use in another dataset).
	'''
	def __init__(self, instances):
		self.instances = instances
		self.used = set()

	def get_instance(self, model, name):
		self.used.add((model, name))
		return self.instances[model][name]

	def used_instances(self):
		used = defaultdict(dict)
		for model, name in self.used:
			used[model][name] = self.instances[model][name]
		return used

class PipelineBase:
	def __init__(self, project_name, *, helper):
		self.project_name = project_name
		self.helper = helper
		self.static_instances = StaticInstanceHolder(self.setup_static_instances())
		self.services = self.setup_services()
		helper.add_services(self.services)
		helper.add_static_instances(self.static_instances)

	def setup_services(self):
		services = {
			'trace_counter': itertools.count(),
			f'fs.data.{self.project_name}': bonobo.open_fs(self.input_path)
		}

		common_path = pathlib.Path(settings.pipeline_common_service_files_path)
		print(f'Common path: {common_path}', file=sys.stderr)
		for file in common_path.rglob('*'):
			service = self._service_from_path(file)
			if service:
				services[file.stem] = service

		proj_path = pathlib.Path(settings.pipeline_project_service_files_path(self.project_name))
		print(f'Project path: {proj_path}', file=sys.stderr)
		for file in proj_path.rglob('*'):
			service = self._service_from_path(file)
			if service:
				if file.stem in services:
					warnings.warn(f'*** Project is overloading a shared service file: {file}')
				services[file.stem] = service

		return services

	def setup_static_instances(self):
		'''
		These are instances that are used statically in the code. For example, when we
		provide attribution of an identifier to Getty, or use a Lugt number, we need to
		serialize the related Group or Person record for that attribution, even if it does
		not appear in the source data.
		'''
		GETTY_GRI_URI = self.helper.make_proj_uri('ORGANIZATION', 'LOCATION-CODE', 'JPGM')
		lugt_ulan = 500321736
		gri_ulan = 500115990
		LUGT_URI = self.helper.make_proj_uri('PERSON', 'ULAN', lugt_ulan)
		gri = model.Group(ident=GETTY_GRI_URI, label='Getty Research Institute')
		gri.identified_by = vocab.PrimaryName(ident='', content='Getty Research Institute')
		gri.exact_match = model.BaseResource(ident=f'http://vocab.getty.edu/ulan/{gri_ulan}')
		lugt = model.Person(ident=LUGT_URI, label='Frits Lugt')
		lugt.identified_by = vocab.PrimaryName(ident='', content='Frits Lugt')
		lugt.exact_match = model.BaseResource(ident=f'http://vocab.getty.edu/ulan/{lugt_ulan}')
		return {
			'Group': {
				'gri': gri
			},
			'Person': {
				'lugt': lugt
			}
		}

	def _service_from_path(self, file):
		if file.suffix == '.json':
			with open(file, 'r') as f:
				try:
					return json.load(f)
				except Exception as e:
					warnings.warn(f'*** Failed to load service JSON: {file}: {e}')
					return None
		elif file.suffix == '.sqlite':
			s = URL(drivername='sqlite', database=file.absolute())
			e = create_engine(s)
			return e

	def get_services(self):
		'''Return a `dict` of named services available to the bonobo pipeline.'''
		return self.services

	def serializer_nodes_for_model(self, model=None, *args, **kwargs):
		nodes = []
		if model:
			nodes.append(AddArchesModel(model=model))
		if self.debug:
			nodes.append(Serializer(compact=False))
		else:
			nodes.append(Serializer(compact=True))
		return nodes

	def add_serialization_chain(self, graph, input_node, model=None, *args, **kwargs):
		'''Add serialization of the passed transformer node to the bonobo graph.'''
		nodes = self.serializer_nodes_for_model(*args, model=model, **kwargs)
		if nodes:
			graph.add_chain(*nodes, _input=input_node)
		else:
			sys.stderr.write('*** No serialization chain defined\n')

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

	def run_graph(self, graph, *, services):
		if True:
			bonobo.run(graph, services=services)
		else:
			e = pipeline.execution.GraphExecutor(graph, services)
			e.run()

class UtilityHelper:
	def __init__(self, project_name):
		self.project_name = project_name
		self.proj_prefix = f'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:{project_name}#'
		self.shared_prefix = f'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#'

	def add_services(self, services):
		'''
		Called from PipelineBase.__init__, this is used for dependency
		injection of services data.
		
		This would ordinarily go in UtilityHelper.__init__, but this object
		is constructed before the pipeline object which is used to produce
		the services dict. So the pipeline object injects the data in *its*
		constructor instead.
		'''
		self.services = services

	def add_static_instances(self, static_instances):
		self.static_instances = static_instances

	def make_uri_path(self, *values):
		return ','.join([urllib.parse.quote(str(v)) for v in values])

	def make_proj_uri(self, *values):
		'''Convert a set of identifying `values` into a URI'''
		if values:
			suffix = self.make_uri_path(*values)
			return self.proj_prefix + suffix
		else:
			suffix = str(uuid.uuid4())
			return self.proj_prefix + suffix

	def make_shared_uri(self, *values):
		'''Convert a set of identifying `values` into a URI'''
		if values:
			suffix = self.make_uri_path(*values)
			return self.shared_prefix + suffix
		else:
			suffix = str(uuid.uuid4())
			return self.shared_prefix + suffix

	def make_place(self, data:dict, base_uri=None):
		'''
		Given a dictionary representing data about a place, construct a model.Place object,
		assign it as the crom data in the dictionary, and return the dictionary.

		The dictionary keys used to construct the place object are:

		- name
		- type (one of: 'City', 'State', 'Province', or 'Country')
		- part_of (a recursive place dictionary)
		'''
		unique_locations = CaseFoldingSet(self.services.get('unique_locations', {}).get('place_names', []))
		TYPES = {
			'city': vocab.instances['city'],
			'province': vocab.instances['province'],
			'state': vocab.instances['province'],
			'country': vocab.instances['nation'],
		}

		if data is None:
			return None
		type_name = data.get('type', 'place').lower()
		name = data['name']
		label = name
		parent_data = data.get('part_of')

		place_type = TYPES.get(type_name)
		parent = None
		if parent_data:
			parent_data = self.make_place(parent_data, base_uri=base_uri)
			parent = get_crom_object(parent_data)
			label = f'{label}, {parent._label}'

		placeargs = {'label': label}
		if data.get('uri'):
			placeargs['ident'] = data['uri']
		elif label in unique_locations:
			data['uri'] = self.make_proj_uri('PLACE', label)
			placeargs['ident'] = data['uri']
		elif base_uri:
			data['uri'] = base_uri + urllib.parse.quote(label)
			placeargs['ident'] = data['uri']

		p = model.Place(**placeargs)
		if place_type:
			p.classified_as = place_type
		if name:
			p.identified_by = model.Name(ident='', content=name)
		else:
			warnings.warn(f'Place with missing name on {p.id}')
		if parent:
			p.part_of = parent
			data['part_of'] = parent_data
		return add_crom_data(data=data, what=p)

	def gri_number_id(self, content, id_class=None):
		if id_class is None:
			id_class = vocab.LocalNumber
		catalog_id = id_class(ident='', content=content)
		assignment = model.AttributeAssignment(ident='')
		assignment.carried_out_by = self.static_instances.get_instance('Group', 'gri')
		catalog_id.assigned_by = assignment
		return catalog_id
