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
			Serializer, \
			Trace

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
		self.anon_period_re = re.compile(r'\[ANONYMOUS - (MODERN|ANTIQUE)\]')
		self.century_span_re = re.compile(r'(\d+)[A-Z]*-?(\d*)', re.IGNORECASE)
		self.unacceptable_century_active_re = re.compile(r'.*\s+BC')

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

	def acceptable_century_active(self, century_active):
		unacceptable = self.unacceptable_century_active_re.match(century_active)
		if unacceptable:
			warnings.warn(f"No timespan information will be populated for {century_active}")
		return not unacceptable

	def is_anonymous_group(self, generic_name):
		return str(generic_name).strip() == 'Yes'

	def is_anonymous(self, data:dict):
		auth_name = data.get('auth_name')
		generic_name = data.get('generic_name')
		if auth_name:
			if self.is_anonymous_group(generic_name):
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
		generic_name = data.get('generic_name', '')
		if auth_name and self.is_anonymous_group(generic_name):
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
		auth_name = a.get('auth_name')
		generic_name = a.get('generic_name')
		# is_group will be true here if this person record is a stand-in
		# for a group of people (e.g. all French people, or 17th century Germans)
		is_group = auth_name and self.is_anonymous_group(generic_name)
		self.add_names(a, group=is_group, referrer=record, **kwargs)
		self.add_props(a, **kwargs)
		if is_group:
			self.make_la_org(a)
		else:
			self.make_la_person(a)
		p = get_crom_object(a)
		if isinstance(record, list):
			for r in record:
				p.referred_to_by = r
		elif record:
			p.referred_to_by = record
		return p

	def add_group(self, a, record=None, relative_id=None, **kwargs):
		self.add_uri(a, record_id=relative_id)
		self.add_names(a, referrer=record, group=True, **kwargs)
		self.add_props(a, **kwargs)
		self.make_la_org(a)
		g = get_crom_object(a)
		if isinstance(record, list):
			for r in record:
				g.referred_to_by = r
		elif record:
			g.referred_to_by = record
		return g

	def add_uri(self, data:dict, **kwargs):
		keys, make = self._uri_keys(data, **kwargs)
		data['uri_keys'] = keys
		data['uri'] = make(*keys)

	def anonymous_group_label(self, role, century_range=None, nationality=None):
		if century_range:
			b, e = century_range
			if b and e and nationality:
				ord_begin = make_ordinal(b)
				ord_end = make_ordinal(e)
				return f'{nationality.capitalize()} {role}s from {ord_begin} to {ord_end} century'
			elif b and e:
				ord_begin = make_ordinal(b)
				ord_end = make_ordinal(e)
				return f'{role}s from {ord_begin} to {ord_end} century'
			elif b and nationality:
				ord = make_ordinal(b)
				return f'{nationality.capitalize()} {role}s in the {ord} century'
			elif b:
				ord = make_ordinal(b)
				return f'{role}s in the {ord} century'
		elif nationality:
			return f'{nationality.capitalize()} {role}s'
		else:
			return f'{role}s'

	def group_label_from_authority_name(self, role, century_range=None, authority_name=None):
		if century_range:
			b, e = century_range
			if b and e and authority_name:
				ord_begin = make_ordinal(b)
				ord_end = make_ordinal(e)
				return f'"{authority_name.title()}" from {ord_begin} to {ord_end} century'
			elif b and e:
				ord_begin = make_ordinal(b)
				ord_end = make_ordinal(e)
				return f'{role}s from {ord_begin} to {ord_end} century'
			elif b and authority_name:
				ord = make_ordinal(b)
				return f'"{authority_name.title()}" in the {ord} century'
			elif b:
				ord = make_ordinal(b)
				return f'{role}s in the {ord} century'
		elif authority_name:
			return f'"{authority_name.title()}"'
		else:
			return f'{role}s'
	
	def make_label_for_professional_activity(self, role: str, authority_name=None, century_range=None, nationality=None):
		if self.acceptable_person_auth_name(auth_name=authority_name):
			return self.group_label_from_authority_name(role, century_range=century_range, authority_name=authority_name)
		else:
			return self.anonymous_group_label(role, century_range=century_range, nationality=nationality)
	
	def century_range_from_century_active(self, century_active: str):
		if self.acceptable_century_active(century_active=century_active):
			century_match = self.century_span_re.match(century_active)
			c_begin = int(century_match.group(1))
			c_end = int(century_match.group(2)) if century_match.group(2) else None
			return [c_begin, c_end]
		else:
			return None

	def professional_activity(self, name:str, century_range=None, date_range=None, classified_as=None, **kwargs):
		'''
		Return a vocab.Active object representing the professional activities
		of the `name`d person.
		
		If `century_range` or `date_range` arguments are supplied, they are used to
		associate a timespan with the activity.
		
		If a `classified_as` list is supplied, it is used to further classify
		the `vocab.Active` object.
		'''
		if not classified_as:
			classified_as = [model.Activity]
		
		args = {'ident': '', 'label': f'Professional activity of {name}'}
		if 'ident' in kwargs:
			args['ident'] = kwargs['ident']
		a = vocab.make_multitype_obj(*classified_as, **args)

		ts = self.active_timespan(century_range=century_range, date_range=date_range, **kwargs)
		if ts:
			if 'verbatim_active_period' in kwargs:
				ts.identified_by = model.Name(ident='', content=kwargs['verbatim_active_period'])
			a.timespan = ts
		return a

	def active_timespan(self, century_range=None, date_range=None, **kwargs):
		'''
		Return a TimeSpan object representing the period during which a
		person was active in their professional activities. If no such
		information is supplied, return None.
		'''
		if century_range:
			b, e = century_range
			ts = timespan_for_century(begin=b, end=e, **kwargs)
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
		period = data.get('period_active_clean')
		century = data.get('century_active_clean')
		birth_pair = data.get('birth_clean')
		death_pair = data.get('death_clean')
		
		if period:
			pass
		else:
			pass
		
		if birth_pair:
			birth = birth_pair[0]
			if 'date_range' in args:
				a_begin, a_end = args['date_range']
				begin = max([d for d in (a_begin, birth) if d is not None])
				args['date_range'] = (begin, a_end)
			elif 'century' in args:
				a_begin, a_end = dates_for_century(args['century'])
				del args['century']
				begin = max([d for d in (a_begin, birth) if d is not None])
				args['date_range'] = (begin, a_end)
		if death_pair:
			death = death_pair[1]
			if 'date_range' in args:
				a_begin, a_end = args['date_range']
				end = min([d for d in (a_end, death) if d is not None])
				args['date_range'] = (a_begin, end)
			elif 'century' in args:
				a_begin, a_end = dates_for_century(args['century'])
				del args['century']
				end = min([d for d in (a_end, death) if d is not None])
				args['date_range'] = (a_begin, end)
		return args

	def active_args(self, data:dict, name:str):
		'''
		Return a dict suitable for passing as keyword arguments to
		`professional_activity` to indicate the time period during
		which a person was active in their professional activities.
		'''
		period_active = data.get('period_active_clean')
		century_active = data.get('century_active_clean')
		cb = data.get('corporate_body')
		if period_active and not cb:
			date_range = period_active
			if date_range:
				return {'date_range': date_range, 'verbatim_active_period': data.get('period_active')}
		elif century_active:
			date_range = century_active
			if date_range:
				return {'date_range': date_range, 'verbatim_active_period': data.get('century_active')}
		return {}

	def add_props(self, data:dict, role=None, split_notes=True, **kwargs):
		role = role if role else 'person'
		auth_name = data.get('auth_name', '')
		generic_name = data.get('generic_name', '')
		century_active = data.get('century_active', '')
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

# 		name = data['label']
# 		active = self.clamped_timespan_args(data, name)
# 		cb = data.get('corporate_body')
# 		if active:
# 			pact_uri = data['uri'] + '-ProfAct-active'
# 			a = self.professional_activity(name, ident=pact_uri, **active)
# 			data['events'].append(a)

		
		notes_field_classification = {
			'brief_notes': (vocab.BiographyStatement, vocab.External),
			'text': (vocab.BiographyStatement, vocab.Internal),
			'working_notes': (vocab.ResearchStatement, vocab.Internal),
		}
		for key, note_classification in notes_field_classification.items():
			if key in data:
				# there's a chance that a `;` separated field might end with a `;`, thus creating an extra entry which is empty
				# the following line splits the field and then filters all empty out
				contents = [n.strip() for n in data[key].split(';') if n.strip()]
				for content in contents:
					cite = vocab.make_multitype_obj(*note_classification, ident='', content=content)
					data['referred_to_by'].append(cite)
		
		if split_notes:
			if 'internal_notes' in data:
				for content in [n.strip() for n in data['internal_notes'].split(';') if n.strip()]:
					cite = vocab.make_multitype_obj(*(vocab.BiographyStatement, vocab.Internal), ident='', content=content)
					data['referred_to_by'].append(cite)
		else:
			if 'internal_notes' in data:
				cite = vocab.make_multitype_obj(*(vocab.BiographyStatement, vocab.Internal), ident='', content=data['internal_notes'])
				data['referred_to_by'].append(cite)
			
		for key in ('name_cite', 'bibliography'):
			if data.get(key):
				cite = vocab.BibliographyStatement(ident='', content=data[key])
				data['referred_to_by'].append(cite)

		if data.get('name_cite'):
			cite = vocab.BibliographyStatement(ident='', content=data['name_cite'])
			data['referred_to_by'].append(cite)

		if self.is_anonymous_group(generic_name):
			data.setdefault('events', [])
			if nationalities and not century_active:
				with suppress(ValueError):
					data['label'] = self.make_label_for_professional_activity(role, authority_name=auth_name, nationality=nationalities[0])
			elif nationalities and century_active:
				with suppress(ValueError):
					c_range = self.century_range_from_century_active(century_active)
					group_label = self.make_label_for_professional_activity(role, authority_name=auth_name, century_range=c_range, nationality=nationalities[0])
					data['label'] = group_label
					pact_uri = data['uri'] + '-ProfAct-dated-natl'
					a = self.professional_activity(group_label, classified_as=[vocab.ActiveOccupation], ident=pact_uri, century_range=c_range, narrow=True)
					data['events'].append(a)
			elif century_active:
				with suppress(ValueError):
					c_range = self.century_range_from_century_active(century_active)
					group_label = self.make_label_for_professional_activity(role, authority_name=auth_name, century_range=c_range)
					data['label'] = group_label
					pact_uri = data['uri'] + '-ProfAct-dated'
					a = self.professional_activity(group_label, classified_as=[vocab.ActiveOccupation], ident=pact_uri, century_range=c_range, narrow=True)
					data['events'].append(a)
			elif period_match:
				period = period_match.group(1).lower()
				data['label'] = f'anonymous {period} {role}s'
		for nationality in nationalities:
			if nationality == "netherlandish":
				nationality = "dutch"
				
			if "and" in nationality or "or" in nationality:
				nx = nationality.split()
				for x in nx:
					if x != "and" and x !="or":
						data = self.add_nationality(x, data)		
			else:
				data = self.add_nationality(nationality, data)
			

	def add_nationality(self, nationality, data):
		key = f'{nationality.lower()} nationality'
		n = vocab.instances.get(key)
		if n:
			data['nationality'].append(n)
		else:
			warnings.warn(f'No nationality instance found in crom for: {key!r}')
		return data

	def add_names(self, data:dict, referrer=None, role=None, group=False, **kwargs):
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
		name_types = [vocab.PrimaryName]
		
		personalNameType = vocab.CorporateName if group else vocab.PersonalName

		if disp_name:
			if auth_name:
				data['identifiers'].append(vocab.PrimaryName(ident='', content=auth_name))
				data['label'] = auth_name
			auth_name = disp_name
			name_types = [personalNameType, vocab.DisplayName]

		role_label = None
		if self.acceptable_person_auth_name(auth_name):
			if role:
				role_label = f'{role} “{auth_name}”'
			data.setdefault('label', auth_name)
			pname = vocab.make_multitype_obj(*name_types, ident='', content=auth_name) # NOTE: most of these are also vocab.SortName, but not 100%, so witholding that assertion for now
			if isinstance(referrer, list):
				for r in referrer:
					pname.referred_to_by = r
			elif referrer:
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
			name_kwargs['classified_as'] = personalNameType
			if isinstance(referrer, list):
				name_kwargs['referred_to_by'] = referrer
			elif referrer:
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

		vocab.register_instance('occupation', {'parent': model.Type, 'id': '300263369', 'label': 'Occupation'})
		vocab.register_instance('function', {'parent': model.Type, 'id': '300444971', 'label': 'Function (general concept)'})
		vocab.register_instance('form type', {'parent': model.Type, 'id': '300444970', 'label': 'Form'})
		vocab.register_instance('object type', {'parent': model.Type, 'id': '300435443', 'label': 'Object / Work Type'})
		vocab.register_vocab_class('StarNumber', {'parent': vocab.LocalNumber, 'id': 'https://data.getty.edu/local/thesaurus/star-identifier', 'label': 'STAR Identifier'})
		vocab.register_vocab_class('CorporateName', {'parent': model.Name, 'id': '300445020', 'label': 'Corporate Name'})
		vocab.register_vocab_class('Internal', {"parent": model.LinguisticObject, "id":"300444972", "label": "private (general concept)", "metatype": "function"})
		vocab.register_vocab_class('External', {"parent": model.LinguisticObject, "id":"300444973", "label": "public (general concept)", "metatype": "function"})
		vocab.register_vocab_class('ActiveOccupation', {"parent": model.Activity, "id":"300393177", "label": "Professional Activities", "metatype": "occupation"})
		vocab.register_vocab_class('Database', {"parent": model.LinguisticObject, "id":"300028543", "label": "Database", "metatype": "form type"})
		vocab.register_vocab_class('Transcription', {"parent": model.LinguisticObject, "id":"300404333", "label": "Transcription", "metatype": "brief text"})
		vocab.register_vocab_class('SellerDescription', {"parent": model.LinguisticObject, "id":"300445025", "label": "seller description", "metatype": "brief text"})
		vocab.register_vocab_class('TitlePageText', {"parent": model.LinguisticObject, "id":"300445697", "label": "title page text", "metatype": "brief text"})
		vocab.register_vocab_class('TranscriptionProcess', {"parent": model.Creation, "id":"300440752", "label": "Transcription Process"})
		vocab.register_vocab_class('AppraisingAssignment', {'parent': model.AttributeAssignment, 'id': '300054622', 'label': 'Appraising'})

		vocab.register_instance('BuyersAgent', {'parent': model.Type, 'id': '300448857', "label": "Buyer's Agent"})
		vocab.register_instance('SellersAgent', {'parent': model.Type, 'id': '300448856', "label": "Seller's Agent"})

		self.static_instances = StaticInstanceHolder(self.setup_static_instances())
		helper.add_static_instances(self.static_instances)


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
		goupil_ulan = 500067127
		goupil_name = 'Goupil et Cie.'
		GETTY_PSCP_URI = self.helper.make_shared_uri('STATIC', 'ORGANIZATION', 'Project for the Study of Collecting and Provenance')
		GETTY_GPI_URI = self.helper.make_shared_uri('STATIC', 'ORGANIZATION', 'Getty Provenance Index')
		GETTY_GRI_URI = self.helper.make_proj_uri('ORGANIZATION', 'LOCATION-CODE', 'JPGM')
		GETTY_GCI_URI = self.helper.make_shared_uri('STATIC', 'ORGANIZATION', 'Getty Conservation Institute')
		LUGT_URI = self.helper.make_proj_uri('PERSON', 'ULAN', lugt_ulan)
		KNOEDLER_URI = self.helper.make_shared_uri('ORGANIZATION', 'ULAN', str(knoedler_ulan))
		GOUPIL_URI = self.helper.make_shared_uri('PERSON', 'AUTH', goupil_name) # produce a uri which is the same as the one produced from the people database
		NEWYORK_URI = self.helper.make_shared_uri('PLACE', 'USA', 'NY', 'New York')

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

		goupil = model.Group(ident=GOUPIL_URI, label=goupil_name)
		goupil.identified_by = vocab.PrimaryName(ident='', content=goupil_name)
		goupil.exact_match = model.BaseResource(ident=f'http://vocab.getty.edu/ulan/{goupil_ulan}')
		
		newyork_name = 'New York, NY'
		newyork = model.Place(ident=NEWYORK_URI, label=newyork_name)
		newyork.identified_by = vocab.PrimaryName(ident='', content=newyork_name)
		
		materials = {}
		if 'materials' in self.services:
			materials.update({
				aat: model.Material(ident=f'http://vocab.getty.edu/aat/{aat}', label=label) for aat, label in self.services['materials'].items()
			})

		places = self._static_place_instances()
		places.update({'newyork': newyork})
		
		db_people = self.static_db_instance('PEOPLE', name='STAR Person Authority Database', creator=gpi)
		db_knoedler = self.static_db_instance('Knoedler', name='STAR Knoedler Database', creator=gpi)
		db_goupil = self.static_db_instance('Goupil', name='STAR Goupil Database', creator=gpi)
		db_sales_events = self.static_db_instance('Sales', 'Descriptions', name='STAR Sales Catalogue Database', creator=gpi)
		db_sales_catalogs = self.static_db_instance('Sales', 'Catalogue', name='STAR Physical Sales Catalogue Database', creator=gpi)
		db_sales_contents = self.static_db_instance('Sales', 'Contents', name='STAR Sales Contents Database', creator=gpi)

		instances = defaultdict(dict)
		instances.update({
			'LinguisticObject': {
				'db-people': db_people,
				'db-knoedler': db_knoedler,
				'db-sales_events': db_sales_events,
				'db-sales_catalogs': db_sales_catalogs,
				'db-sales_contents': db_sales_contents,
				'db-goupil' : db_goupil
			},
			'Group': {
				'gci': gci,
				'pscp': pscp,
				'gri': gri,
				'gpi': gpi,
				'knoedler': knoedler,
				'goupil' : goupil
			},
			'Person': {
				'lugt': lugt
			},
			'Material': materials,
			'Place': places
		})
		
		return instances

	def static_db_instance(self, *keys, **kwargs):
		uri = self.helper.make_shared_uri('DB', *keys)
		label = ' '.join(keys)
		name = kwargs.get('name', f'STAR {label} Database')
		db = vocab.Database(ident=uri, label=name)
		db.identified_by = vocab.PrimaryName(ident='', content=name)
		er_classification = model.Type(ident='http://vocab.getty.edu/aat/300379790', label='Electronic Records')
		er_classification.classified_as = vocab.instances["object type"]
		db.classified_as = er_classification
		creator = kwargs.get('creator')
		if creator:
			creation = model.Creation(ident='')
			creation.carried_out_by = creator
			db.created_by = creation
		return db

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
			s = URL.create(drivername='sqlite', database=str(file.absolute()))
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

	def add_places_chain(self, graph, auction_events, key='_locations', serialize=True, **kwargs):
		'''Add extraction and serialization of locations.'''
		nodes = []
		if key:
			nodes.append(ExtractKeyedValues(key=key))
		nodes.append(RecursiveExtractKeyedValue(key='part_of', **kwargs))
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
		for n in list(self.canonical_location_names.values()):
			self.canonical_location_names[n.casefold()] = n

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

	def get_canonical_place(self, name):
		if name is None:
			return None
		canonical_location_names = self.canonical_location_names
		if name.casefold() in canonical_location_names:
			name = canonical_location_names.get(name.casefold(), name)
		si = self.static_instances
		if si:
			return si.get_instance('Place', name)
		return None
		
	def make_place(self, data:dict, base_uri=None, record=None):
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
			'address': vocab.instances['address'],
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
			parent_data = self.make_place(parent_data, base_uri=base_uri, record=record)
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
						parent_data = self.make_place(place_data['part_of'], base_uri=base_uri, record=record)
						queue.append(parent_data)
		if p:
			if record:
				p.referred_to_by = record
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
			if record:
				p.referred_to_by = record
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

	def goupil_gpi_number_id(self, content, id_class=None):
		if id_class is None:
			id_class = vocab.StarNumber
		catalog_id = id_class(ident='', content=content)
		assignment = model.AttributeAssignment(ident='')
		assignment.carried_out_by = self.static_instances.get_instance('Group', 'gpi')
		catalog_id.assigned_by = assignment
		return catalog_id

	def knoedler_number_id(self, content, id_class=None):
		if id_class is None:
			id_class = vocab.LocalNumber
		k_id = id_class(ident='', content=content)
		assignment = model.AttributeAssignment(ident='')
		assignment.carried_out_by = self.static_instances.get_instance('Group', 'knoedler')
		k_id.assigned_by = assignment
		return k_id

	def goupil_number_id(self, content, id_class=None, assignment_label=None):
		if id_class is None:
			id_class = vocab.LocalNumber
		g_id = id_class(ident='', content=content)
		assignment = model.AttributeAssignment(ident='', label=assignment_label)
		assignment.carried_out_by = self.static_instances.get_instance('Group', 'goupil')
		g_id.assigned_by = assignment
		return g_id

	def goupil_pscp_number_id(self, content, id_class=None, assignment_label=None):
		if id_class is None:
			id_class = vocab.LocalNumber
		g_id = id_class(ident='', content=content)
		assignment = model.AttributeAssignment(ident='', label=assignment_label)
		assignment.carried_out_by = self.static_instances.get_instance('Group', 'gri')
		g_id.assigned_by = assignment
		return g_id
		
	def add_group(self, data, **kwargs):
		return self.person_identity.add_group(data, **kwargs)

	def add_person(self, data, **kwargs):
		return self.person_identity.add_person(data, **kwargs)
	