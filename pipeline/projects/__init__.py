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
			RecursiveExtractKeyedValue, \
			timespan_for_century, \
			dates_for_century, \
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
		self.anon_nationality_re = re.compile(r'\[(?!ANON|ILLEGIBLE|Unknown)(\w+)\]', re.IGNORECASE)

	def acceptable_person_auth_name(self, auth_name):
		if not auth_name:
			return False
		if not auth_name or auth_name in self.ignore_authnames:
			return False
		elif '[UNIDENTIFIED]' in auth_name:
			# these are a special case in PEOPLE.
			# there are ~7k records with auth names containing '[UNIDENTIFIED]'
			return True
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

		for anon_key in ('[ILLEGIBLE]', '[Unknown]'):
			if anon_key == auth_name:
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

		if auth_name and self.is_anonymous_group(auth_name):
			key = ('GROUP', 'AUTH', auth_name)
			return key, self.make_shared_uri
		elif auth_name and self.acceptable_person_auth_name(auth_name):
			key = ('PERSON', 'AUTH', auth_name)
			return key, self.make_shared_uri
		elif ulan:
			key = ('PERSON', 'ULAN', ulan)
			return key, self.make_shared_uri
		else:
			# not enough information to identify this person uniquely, so use the source location in the input file
			try:
				if 'pi_record_no' in data:
					id_value = data['pi_record_no']
					id_key = 'PI'
				else:
					id_value = data['star_record_no']
					id_key = 'STAR'
			except KeyError as e:
				warnings.warn(f'*** No identifying property with which to construct a URI key: {e}')
				print(pprint.pformat(data), file=sys.stderr)
				raise
			if record_id:
				key = ('PERSON', id_key, id_value, record_id)
				return key, self.make_proj_uri
			else:
				warnings.warn(f'*** No record identifier given for person identified only by {id_key} {id_value}')
				key = ('PERSON', id_key, id_value)
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

	def add_group(self, a, record=None, relative_id=None, **kwargs):
		self.add_uri(a, record_id=relative_id)
		self.add_names(a, referrer=record, **kwargs)
		self.add_props(a, **kwargs)
		self.make_la_org(a)
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
		
	def professional_activity(self, name:str, century=None, date_range=None, classified_as=None, **kwargs):
		'''
		Return a vocab.Active object representing the professional activities
		of the `name`d person.
		
		If `century` or `date_range` arguments are supplied, they are used to
		associate a timespan with the activity.
		
		If a `classified_as` list is supplied, it is used to further classify
		the `vocab.Active` object.
		'''
		if classified_as:
			classified_as.append(vocab.Active)
		else:
			classified_as = [vocab.Active]
		
		args = {'ident': '', 'label': f'Professional activity of {name}'}
		if 'ident' in kwargs:
			args['ident'] = kwargs['ident']
		a = vocab.make_multitype_obj(*classified_as, **args)

		ts = self.active_timespan(century=century, date_range=date_range, **kwargs)
		if ts:
			a.timespan = ts
		return a

	def active_timespan(self, century=None, date_range=None, **kwargs):
		'''
		Return a TimeSpan object representing the period during which a
		person was active in their professional activities. If no such
		information is supplied, return None.
		'''
		if century:
			ts = timespan_for_century(century, **kwargs)
			return ts
		elif date_range:
			b, e = date_range
			ts = timespan_from_outer_bounds(begin=b, end=e, inclusive=True)
			return ts
		return None

	def clamped_timespan_args(self, data:dict, name:str):
		'''
		This is shorthand for calling
		
		  clamp_timespan_args_to_lifespan(data, self.active_args(data, name)
		
		'''
		return self.clamp_timespan_args_to_lifespan(data, self.active_args(data, name))
		
	def clamp_timespan_args_to_lifespan(self, data:dict, args:dict):
		'''
		Given a dict `data` containing keyword arguments for passing to
		`professional_activity()`, which contains information on a time
		period during which a person was active (e.g. derived from data
		indicating that they were active in the 18th century), return a
		new keyword argument dict that contains equivalent information
		except with the bounds of the timespan narrowed to encompass only
		the person's lifespan (if known).
		'''
		birth_pair = data.get('birth_clean')
		death_pair = data.get('death_clean')
		if birth_pair and death_pair:
			birth = birth_pair[0]
			death = death_pair[1]
			if 'date_range' in args:
				a_begin, a_end = args['date_range']
				begin = max([d for d in (a_begin, birth) if d is not None])
				end = min([d for d in (a_end, death) if d is not None])
				args['date_range'] = (begin, end)
			elif 'century' in args:
				a_begin, a_end = dates_for_century(args['century'])
				del args['century']
				begin = max([d for d in (a_begin, birth) if d is not None])
				end = min([d for d in (a_end, death) if d is not None])
				args['date_range'] = (begin, end)
		return args

	def active_args(self, data:dict, name:str):
		'''
		Return a dict suitable for passing as keyword arguments to
		`professional_activity` to indicate the time period during
		which a person was active in their professional activities.
		'''
		period_active = data.get('period_active')
		century_active = data.get('century_active')
		if period_active:
			date_range = date_cleaner(period_active)
			if date_range:
				return {'date_range': date_range}
		elif century_active:
			if len(century_active) == 4 and century_active.endswith('th'):
				century = int(century_active[0:2])
				return {'century': century}
			else:
				warnings.warn(f'TODO: better handling for century ranges: {century_active}')
		return {}

	def add_props(self, data:dict, role=None, **kwargs):
		role = role if role else 'person'
		auth_name = data.get('auth_name', '')
		period_match = self.anon_period_re.match(auth_name)
		nationalities = []
		if 'nationality' in data:
			nationality = data['nationality']
			if isinstance(nationality, str):
				nationalities += [n.lower().strip() for n in nationality.split(';')]
			elif isinstance(nationality, list):
				nationalities += [n.lower() for n in nationality]

		data['nationality'] = []
		data.setdefault('referred_to_by', [])
		
		name = data['label']
		active = self.clamped_timespan_args(data, name)
		if active:
			pact_uri = data['uri'] + '-ProfAct-active'
			a = self.professional_activity(name, ident=pact_uri, **active)
			data['events'].append(a)

		notes_field_classification = {
			'brief_notes': (vocab.BiographyStatement, vocab.External),
			'text': (vocab.BiographyStatement, vocab.Internal),
			'internal_notes': (vocab.BiographyStatement, vocab.Internal),
			'working_notes': (vocab.ResearchStatement, vocab.Internal),
		}
		for key, note_classification in notes_field_classification.items():
			if key in data:
				for content in [n.strip() for n in data[key].split(';')]:
					cite = vocab.make_multitype_obj(*note_classification, ident='', content=content)
					data['referred_to_by'].append(cite)

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
			data.setdefault('events', [])
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
					pact_uri = data['uri'] + '-ProfAct-dated-natl'
					a = self.professional_activity(group_label, ident=pact_uri, century=century, narrow=True)
					data['events'].append(a)
			elif dated_match:
				with suppress(ValueError):
					century = int(dated_match.group(1))
					group_label = self.anonymous_group_label(role, century=century)
					data['label'] = group_label
					pact_uri = data['uri'] + '-ProfAct-dated'
					a = self.professional_activity(group_label, ident=pact_uri, century=century, narrow=True)
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
		data.setdefault('identifiers', [])
		auth_name = data.get('auth_name', '')
		disp_name = data.get('auth_display_name')
		name_type = vocab.PrimaryName
		
		if disp_name:
			if auth_name:
				data['identifiers'].append(vocab.PrimaryName(ident='', content=auth_name))
			auth_name = disp_name
			name_type = vocab.Name

		role_label = None
		if self.acceptable_person_auth_name(auth_name):
			if role:
				role_label = f'{role} “{auth_name}”'
			data['label'] = auth_name
			pname = name_type(ident='', content=auth_name) # NOTE: most of these are also vocab.SortName, but not 100%, so witholding that assertion for now
			if referrer:
				pname.referred_to_by = referrer
			data['identifiers'].append(pname)

		data.setdefault('names', [])

		names = []
		name = data.get('name')
		if name:
			del data['name'] # this will be captured in the 'names' array, so remove it here so the output isn't duplicated
			names.append(name)
		variant_names = data.get('variant_names')
		if variant_names:
			names += [n.strip() for n in variant_names.split(';')]

		for name in names:
			if role and not role_label:
				role_label = f'{role} “{name}”'
			name_kwargs = {}
# 			name_kwargs['classified_as'] = model.Type(ident='http://vocab.getty.edu/aat/300266386', label='Personal Name')
			name_kwargs['classified_as'] = vocab.PersonalName
			if referrer:
				name_kwargs['referred_to_by'] = [referrer]
			data['names'].append((name, name_kwargs))
			data.setdefault('label', name)
		data.setdefault('label', '(Anonymous)')

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
		m = self.instances.get(model)
		if not m:
			return None
		i = m.get(name)
		if i:
			self.used.add((model, name))
			return i
		return None

	def used_instances(self):
		used = defaultdict(dict)
		for model, name in self.used:
			used[model][name] = self.instances[model][name]
		return used

class PipelineBase:
	def __init__(self, project_name, *, helper, parallel=False, verbose=False, **kwargs):
		self.project_name = project_name
		self.parallel = parallel
		self.helper = helper
		self.verbose = verbose
		self.services = self.setup_services()
		helper.add_services(self.services)
		self.static_instances = StaticInstanceHolder(self.setup_static_instances())
		helper.add_static_instances(self.static_instances)
		vocab.register_vocab_class('StarNumber', {'parent': vocab.LocalNumber, 'id': 'http://ns.getty.edu/pipeline-star-number', 'label': 'STAR-assigned Number'})
		vocab.register_instance('function', {'parent': model.Type, 'id': '300444971', 'label': 'Function (general concept)'})
		vocab.register_vocab_class('Internal', {"parent": model.LinguisticObject, "id":"300444972", "label": "private (general concept)", "metatype": "function"})
		vocab.register_vocab_class('External', {"parent": model.LinguisticObject, "id":"300444973", "label": "public (general concept)", "metatype": "function"})


	def setup_services(self):
		services = {
			'trace_counter': itertools.count(),
			f'fs.data.{self.project_name}': bonobo.open_fs(self.input_path)
		}

		common_path = pathlib.Path(settings.pipeline_common_service_files_path)
		if self.verbose:
			print(f'Common path: {common_path}', file=sys.stderr)
		for file in common_path.rglob('*'):
			service = self._service_from_path(file)
			if service:
				services[file.stem] = service

		proj_path = pathlib.Path(settings.pipeline_project_service_files_path(self.project_name))
		if self.verbose:
			print(f'Project path: {proj_path}', file=sys.stderr)
		for file in proj_path.rglob('*'):
			service = self._service_from_path(file)
			if service:
				if file.stem in services:
					warnings.warn(f'*** Project is overloading a shared service file: {file}')
				services[file.stem] = service

		# re-arrange the materials map service data to use a tuple as the dictionary key
		mm = {}
		for v in services.get('materials_map', []):
			otype = v['object_type']
			m = v['materials']
			if ';' in m:
				m = frozenset([m.strip() for m in m.split(';')])
			else:
				m = frozenset([m])
			key = (otype, m)
			mm[key] = v
		services['materials_map'] = mm
		
		return services

	def setup_static_instances(self):
		'''
		These are instances that are used statically in the code. For example, when we
		provide attribution of an identifier to Getty, or use a Lugt number, we need to
		serialize the related Group or Person record for that attribution, even if it does
		not appear in the source data.
		'''
		lugt_ulan = 500321736
		gri_ulan = 500115990
		gci_ulan = 500115991
		knoedler_ulan = 500304270
		GETTY_PSCP_URI = self.helper.make_shared_uri('STATIC', 'ORGANIZATION', 'Project for the Study of Collecting and Provenance')
		GETTY_GPI_URI = self.helper.make_shared_uri('STATIC', 'ORGANIZATION', 'Getty Provenance Index')
		GETTY_GRI_URI = self.helper.make_proj_uri('ORGANIZATION', 'LOCATION-CODE', 'JPGM')
		GETTY_GCI_URI = self.helper.make_shared_uri('STATIC', 'ORGANIZATION', 'Getty Conservation Institute')
		LUGT_URI = self.helper.make_proj_uri('PERSON', 'ULAN', lugt_ulan)
		KNOEDLER_URI = self.helper.make_shared_uri('ORGANIZATION', 'ULAN', str(knoedler_ulan))

		gci = model.Group(ident=GETTY_GCI_URI, label='Getty Conservation Institute')
		gci.identified_by = vocab.PrimaryName(ident='', content='Getty Conservation Institute')
		gci.exact_match = model.BaseResource(ident=f'http://vocab.getty.edu/ulan/{gci_ulan}')

		gri = model.Group(ident=GETTY_GRI_URI, label='Getty Research Institute')
		gri.identified_by = vocab.PrimaryName(ident='', content='Getty Research Institute')
		gri.exact_match = model.BaseResource(ident=f'http://vocab.getty.edu/ulan/{gri_ulan}')

		gpi = model.Group(ident=GETTY_GPI_URI, label='Getty Provenance Index')
		gpi.identified_by = vocab.PrimaryName(ident='', content='Getty Provenance Index')

		pscp = model.Group(ident=GETTY_PSCP_URI, label='Project for the Study of Collecting and Provenance')
		pscp.identified_by = vocab.PrimaryName(ident='', content='Project for the Study of Collecting and Provenance')

		lugt = model.Person(ident=LUGT_URI, label='Frits Lugt')
		lugt.identified_by = vocab.PrimaryName(ident='', content='Frits Lugt')
		lugt.exact_match = model.BaseResource(ident=f'http://vocab.getty.edu/ulan/{lugt_ulan}')

		knoedler_name = 'M. Knoedler & Co.'
		knoedler = model.Group(ident=KNOEDLER_URI, label=knoedler_name)
		knoedler.identified_by = vocab.PrimaryName(ident='', content=knoedler_name)
		knoedler.exact_match = model.BaseResource(ident=f'http://vocab.getty.edu/ulan/{knoedler_ulan}')

		materials = {}
		if 'materials' in self.services:
			materials.update({
				aat: model.Material(ident=f'http://vocab.getty.edu/aat/{aat}', label=label) for aat, label in self.services['materials'].items()
			})

		instances = defaultdict(dict)
		instances.update({
			'Group': {
				'gci': gci,
				'pscp': pscp,
				'gri': gri,
				'gpi': gpi,
				'knoedler': knoedler
			},
			'Person': {
				'lugt': lugt
			},
			'Material': materials,
			'Place': self._static_place_instances()
		})
		
		return instances

	def _static_place_instances(self):
		'''
		Create static instances for every place mentioned in the unique_locations service data.
		'''
		places = self.helper.services.get('unique_locations', {}).get('places', {})
		instances = {}
		for name, data in places.items():
			place_data = None
			place = None
			components = []
			for k in ('Sovereign', 'Country', 'Province', 'State', 'County', 'City'):
				component_name = data.get(k.lower())
				if component_name:
					components = [component_name] + components
					rev = components
					rev.reverse()
					place_data = {
						'name': component_name,
						'type': k,
						'part_of': place_data,
						'uri': self.helper.make_shared_uri('PLACE', *rev)
					}
					parent = place
					place_data = self.helper.make_place(place_data)
					place = get_crom_object(place_data)
					instances[', '.join(components)] = place
			if place:
				instances[name] = place
		return instances

	def _service_from_path(self, file):
		if file.suffix == '.json':
			with open(file, 'r', encoding='utf-8') as f:
				try:
					return json.load(f)
				except Exception as e:
					warnings.warn(f'*** Failed to load service JSON: {file}: {e}')
					return None
		elif file.suffix == '.sqlite':
			s = URL(drivername='sqlite', database=file.absolute())
			e = create_engine(s)
			return e

	def get_services(self, **kwargs):
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

	def add_places_chain(self, graph, auction_events, key='_locations', serialize=True):
		'''Add extraction and serialization of locations.'''
		nodes = []
		if key:
			nodes.append(ExtractKeyedValues(key=key))
		nodes.append(RecursiveExtractKeyedValue(key='part_of'))
		places = graph.add_chain(
			*nodes,
			_input=auction_events.output
		)
		if serialize:
			# write OBJECTS data
			self.add_serialization_chain(graph, places.output, model=self.models['Place'])
		return places

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
		if self.parallel:
			if self.verbose:
				print('Running with PARALLEL bonobo executor')
			bonobo.run(graph, services=services)
		else:
			if self.verbose:
				print('Running with SERIAL custom executor')
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
		self.static_instances = None
		self.canonical_location_names = {k.casefold(): v for k, v in self.services.get('unique_locations', {}).get('canonical_names', {}).items()}

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

	def prepend_uri_key(self, uri, key):
		return uri.replace('#', f'#{key},')

	def make_place(self, data:dict, base_uri=None):
		'''
		Given a dictionary representing data about a place, construct a model.Place object,
		assign it as the crom data in the dictionary, and return the dictionary.

		The dictionary keys used to construct the place object are:

		- name
		- type (one of: 'City', 'State', 'Province', 'Country', or 'Sovereign')
		- part_of (a recursive place dictionary)
		
		If the name matches a known unique location (derived from the unique_locations
		service data), the normal recursive handling of part_of data is bypassed, using
		the 
		'''
# 		unique_locations = self.unique_locations
		canonical_location_names = self.canonical_location_names
		TYPES = {
			'city': vocab.instances['city'],
			'county': vocab.instances['county'],
			'province': vocab.instances['province'],
			'state': vocab.instances['province'],
			'country': vocab.instances['nation'],
			'sovereign': vocab.instances['sovereign'],
		}

		if data is None:
			return None
		type_name = data.get('type', 'place').lower()
		
		name = data.get('name')
		si = self.static_instances

		names = data.get('names', [])
		label = name
		parent_data = data.get('part_of')

		place_type = TYPES.get(type_name)
		
		parent = None
		
		if name.casefold() in canonical_location_names:
			name = canonical_location_names.get(name.casefold(), name)
			label = name
		elif parent_data:
			parent_data = self.make_place(parent_data, base_uri=base_uri)
			parent = get_crom_object(parent_data)
			if label:
				label = f'{label}, {parent._label}'


		placeargs = {}
		p = None
		if si:
			p = si.get_instance('Place', name)
			if not p:
				p = si.get_instance('Place', label)
				
			if p:
				# this is a static instance. we need to re-thread the part_of relationship
				# in the data dictionary, because the serialization depends on the dictionary
				# data, not the properties of the modeled object
# 				from cromulent.model import factory
# 				print(f'PLACE: {name} => {factory.toString(p, False)}')
				add_crom_data(data=data, what=p)
				queue = [data]
				while queue:
					place_data = queue.pop(0)
					place = get_crom_object(place_data)
					parents = getattr(place, 'part_of', []) or []
					if parents:
						for parent in parents:
							if parent:
								if 'part_of' not in place_data:
									parent_data = add_crom_data(data={}, what=parent)
									place_data['part_of'] = parent_data
								else:
									parent_data = add_crom_data(data=place_data['part_of'], what=parent)
								queue.append(parent_data)
					elif 'part_of' in place_data:
						parent_data = self.make_place(place_data['part_of'], base_uri=base_uri)
						queue.append(parent_data)
		if p:
			return data

		if label:
			placeargs['label'] = label

		if data.get('uri'):
			placeargs['ident'] = data['uri']
# 		elif label.casefold() in canonical_location_names:
# 			label = canonical_location_names[label.casefold()]
# 			data['uri'] = self.make_shared_uri('PLACE', label)
# 			placeargs['ident'] = data['uri']
		elif base_uri:
			data['uri'] = base_uri + urllib.parse.quote(label)
			placeargs['ident'] = data['uri']

		if not p:
			p = model.Place(**placeargs)
			if place_type:
				p.classified_as = place_type
			if name:
				p.identified_by = vocab.PrimaryName(ident='', content=name)
			else:
				warnings.warn(f'Place with missing name on {p.id}')
			for name in names:
				if name:
					p.identified_by = model.Name(ident='', content=name)
			if parent:
				p.part_of = parent
				data['part_of'] = parent_data
		return add_crom_data(data=data, what=p)

	def gci_number_id(self, content, id_class=None):
		if id_class is None:
			id_class = vocab.LocalNumber
		catalog_id = id_class(ident='', content=content)
		assignment = model.AttributeAssignment(ident=self.make_shared_uri('__gci_attribute_assignment'))
		assignment.carried_out_by = self.static_instances.get_instance('Group', 'gci')
		catalog_id.assigned_by = assignment
		return catalog_id

	def gri_number_id(self, content, id_class=None):
		if id_class is None:
			id_class = vocab.LocalNumber
		catalog_id = id_class(ident='', content=content)
		assignment = model.AttributeAssignment(ident=self.make_shared_uri('__gri_attribute_assignment'))
		assignment.carried_out_by = self.static_instances.get_instance('Group', 'gri')
		catalog_id.assigned_by = assignment
		return catalog_id

	def gpi_number_id(self, content, id_class=None):
		if id_class is None:
			id_class = vocab.StarNumber
		catalog_id = id_class(ident='', content=content)
		assignment = model.AttributeAssignment(ident=self.make_shared_uri('__gpi_attribute_assignment'))
		assignment.carried_out_by = self.static_instances.get_instance('Group', 'gpi')
		catalog_id.assigned_by = assignment
		return catalog_id

	def pscp_number_id(self, content, id_class=None):
		if id_class is None:
			id_class = vocab.StarNumber
		catalog_id = id_class(ident='', content=content)
		assignment = model.AttributeAssignment(ident=self.make_shared_uri('__pscp_attribute_assignment'))
		assignment.carried_out_by = self.static_instances.get_instance('Group', 'pscp')
		catalog_id.assigned_by = assignment
		return catalog_id

	def knoedler_number_id(self, content, idclass=None):
		if idclass is None:
			idclass = vocab.LocalNumber
		k_id = idclass(ident='', content=content)
		assignment = model.AttributeAssignment(ident='')
		assignment.carried_out_by = self.static_instances.get_instance('Group', 'knoedler')
		k_id.assigned_by = assignment
		return k_id

	def add_group(self, data, **kwargs):
		return self.person_identity.add_group(data, **kwargs)

	def add_person(self, data, **kwargs):
		return self.person_identity.add_person(data, **kwargs)
	