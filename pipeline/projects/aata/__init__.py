'''
Classes and utility functions for instantiating, configuring, and
running a bonobo pipeline for converting AATA XML data into JSON-LD.
'''

# AATA Extracters

import sys
import pprint
import itertools
import warnings

import json
import iso639
import lxml.etree
from sqlalchemy import create_engine
from langdetect import detect
import urllib.parse
from datetime import datetime
import bonobo
from bonobo.config import Configurable, Service, Option, use
from bonobo.constants import NOT_MODIFIED
import xmltodict

import settings
from cromulent import model, vocab
from cromulent.model import factory
from pipeline.projects import PipelineBase, UtilityHelper
from pipeline.util import identity, \
			GraphListSource, \
			ExtractKeyedValue, \
			ExtractKeyedValues, \
			MatchingFiles, \
			timespan_from_outer_bounds
from pipeline.io.file import MultiFileWriter, MergingFileWriter
# from pipeline.io.arches import ArchesWriter
from pipeline.linkedart import \
			MakeLinkedArtAbstract, \
			MakeLinkedArtLinguisticObject, \
			MakeLinkedArtOrganization, \
			MakeLinkedArtPerson, \
			get_crom_object, \
			add_crom_data
from pipeline.io.xml import CurriedXMLReader
from pipeline.nodes.basic import \
			AddArchesModel, \
			CleanDateToSpan, \
			Serializer, \
			Trace
from pipeline.util.cleaners import ymd_to_datetime

from pipeline.projects.aata.articles import ModelArticle
from pipeline.projects.aata.people import ModelPerson
from pipeline.projects.aata.journals import ModelJournal
from pipeline.projects.aata.series import ModelSeries
from pipeline.projects.aata.places import ModelPlace
from pipeline.projects.aata.corps import ModelCorp

legacyIdentifier = None # TODO: aat:LegacyIdentifier?
doiIdentifier = vocab.DoiIdentifier
variantTitleIdentifier = vocab.Identifier # TODO: aat for variant titles?

# utility functions

class AATAUtilityHelper(UtilityHelper):
	def __init__(self, project_name):
		super().__init__(project_name)

	def document_type_class(self, code):
		document_types = self.services['document_types']
		clsname = document_types[code]
		return getattr(vocab, clsname)

	def role_type(self, role):
		author_roles = self.services['author_roles']
		code = author_roles.get(role)
		if code:
			t = model.Type(ident='http://vocab.getty.edu/aat/' + code, label=role)
			return t
		return None

	def ordered_author_string(self, authors):
		if not authors:
			return None
		elif len(authors) == 1:
			return authors[0]
		elif len(authors) == 2:
			return ' and '.join(authors)
		else:
			r = authors.pop()
			l = authors.pop()
			authors.append(' and '.join([l, r]))
			return ', '.join(authors)

	def language_object_from_code(self, code):
		'''
		Given a three-letter language code (which are mostly drawn from ISO639-2, with some
		exceptions), return a model.Language object for the corresponding language.

		For example, `language_object_from_code('eng')` returns an object representing
		the English language.
		'''
		language_code_map = self.services['language_code_map']
		try:
			if code == 'unk': # TODO: verify that 'unk' is 'unknown' and can be skipped
				return None
			if code.lower() in vocab.instances:
				return vocab.instances[code.lower()]
			elif code in language_code_map:
				language_name = language_code_map[code]
				try:
					return vocab.instances[language_name]
				except KeyError:
					if settings.DEBUG:
						warnings.warn(f'*** No AAT language instance found: {language_name!r}')
			else:
				if settings.DEBUG:
					warnings.warn(f'*** No AAT link for language {code!r}')
		except Exception as e:
			print(f'*** language_object_from_code: {e}', file=sys.stderr)
			raise e

	def validated_string_language(self, title, restrict=None):
		try:
			restrict_codes = {iso639.to_iso639_2(l) for l in restrict}
			detected = detect(title)
			threealpha = iso639.to_iso639_2(detected)
			ok = True if restrict is None else (threealpha in restrict_codes)
			if ok:
				language = self.language_object_from_code(threealpha)
				if language is not None:
					# If there was a restricted set of languages passed to this function,
					# we have confidence that we've matched the language of the title
					# because it is one of the restricted languages.
					# (If no restriction was supplied, then we use whatever language
					# was detected in the title.)
					return language
			else:
				# The detected language of the title was not a member of the restricted
				# set, so we lack confidence to proceed.
				return None
		except iso639.NonExistentLanguageError as e:
			warnings.warn('*** Unrecognized language code detected: %s' % (e,))
		except KeyError as e:
			warnings.warn(
				'*** LANGUAGE: detected but unrecognized title language %r '
				'(cf. declared in metadata: %r): %s' % (e.args[0], languages, title)
			)
		except Exception as e:
			print('*** detect_title_language error: %r' % (e,))
		return None

	def article_uri(self, a_id):
		return self.make_proj_uri('Article', a_id)

	def corporate_body_uri(self, corp_id):
		return self.make_proj_uri('Corp', corp_id)

	def person_uri(self, p_id):
		return self.make_proj_uri('Person', 'GAIA', p_id)

	def place_uri(self, geog_id):
		return self.make_proj_uri('Place', geog_id)

	def journal_uri(self, j_id):
		return self.make_proj_uri('Journal', j_id)

	def series_uri(self, s_id):
		return self.make_proj_uri('Series', s_id)

	def issue_uri(self, j_id, i_id):
		return self.make_proj_uri('Journal', j_id, 'Issue', i_id)

def _xml_element_to_dict(e):
	chunk = lxml.etree.tostring(e).decode('utf-8')
	data = xmltodict.parse(chunk)
	s = json.dumps(data)
	data = json.loads(s)
	return data

# def _gaia_authority_type(code):
# 	if code in ('CB', 'Corp'):
# 		return model.Group
# 	elif code in ('PN', 'Person'):
# 		return model.Person
# 	elif code == 'GP':
# 		return model.Place
# 	elif code == 'SH':
# 		return model.Type
# 	elif code == 'CX':
# 		# TODO: handle authority
# 		return model.Type
# 	elif code == 'TAL':
# 		# TODO: handle authority
# 		return model.Type
# 	else:
# 		warnings.warn(f'Not a recognized authority type code: {code}')
# 		raise LookupError

def add_imprint_orgs(data):
	'''
	Given a `dict` representing an "article," extract the "imprint organization" records
	and their role (e.g. publisher, distributor), and add add a new 'organizations' key
	to the dictionary containing an array of `dict`s representing the organizations.
	Also construct an Activity for each organization's role, and associate it with the
	article and organization (article --role--> organization).

	The resulting organization `dict` will contain these keys:

	* `_aata_record_id`: The identifier of the corresponding article
	* `_aata_record_organization_seq`: A integer identifying this organization
	                                   (unique within the scope of the article)
	* `label`: The name of the organization
	* `role`: The role the organization played in the article's creation (e.g. `'Publishing'`)
	* `properties`: A `dict` of additional properties associated with this organization's
	                role in the article creation (e.g. `DatesOfPublication`)
	* `names`: A `list` of names this organization may be identified by
	* `identifiers`: A `list` of (identifier, identifier type) pairs
	* `uid`: A unique ID for this organization
	* `uuid`: A unique UUID for this organization used in assigning it a URN

	'''
	lod_object = get_crom_object(data)
	organizations = []
	for o in data.get('_organizations', []):
		org = {k: v for k, v in o.items()}
		org_obj = vocab.Group(ident=org['uri'])
		add_crom_data(data=org, what=org_obj)

		event = model.Activity() # TODO: change to vocab.Publishing for publishing activities
		lod_object.used_for = event
		event.carried_out_by = org_obj

		properties = o.get('properties')
		role = o.get('role')
		if role is not None:
			activity_names = {
				'distributor': 'Distributing',
				'publisher': 'Publishing',
				# TODO: Need to also handle roles: Organization, Sponsor, University
			}
			if role.lower() in activity_names:
				event_label = activity_names[role.lower()]
				event._label = event_label
			else:
				print('*** No/unknown organization role (%r) found for imprint_group in %s:' % (
					role, lod_object,))
# 				pprint.pprint(o)

			if role == 'Publisher' and 'DatesOfPublication' in properties:
				pubdate = properties['DatesOfPublication']
				span = CleanDateToSpan.string_to_span(pubdate)
				if span is not None:
					event.timespan = span
		organizations.append(org)
	data['organizations'] = organizations
	return data

def make_aata_org_event(o: dict):
	'''
	Given a `dict` representing an organization, create an `model.Activity` object to
	represent the organization's part in the "article" creation (associating any
	applicable publication timespan to the activity), associate the activity with the
	organization and the corresponding "article", and return a new `dict` that combines
	the input data with an `'events'` key having a `list` value containing the new
	activity object.

	For example,

	```
	make_aata_org_event({
		'event_label': 'Publishing',
		'publication_date_span': model.TimeSpan(...),
		...
	})
	```

	will return:

	```
	{
		'event_label': 'Publishing',
		'publication_date_span': model.TimeSpan(...),
		'events': [model.Activity(_label: 'Publishing', 'timespan': ts.TimeSpan(...))],
		...
	}
	```

	and also set the article object's `used_for` property to the new activity.
	'''
	event = model.Activity()
	lod_object = get_crom_object(o['parent_data'])
	lod_object.used_for = event
	event._label = o.get('event_label')
	if 'publication_date_span' in o:
		ts = o['publication_date_span']
		event.timespan = ts
	org = {k: v for k, v in o.items()}
	org.update({
		'events': [event],
	})
	yield org

# article authors chain

def add_aata_authors(data):
	'''
	Given a `dict` representing an "article," extract the authorship records
	and their role (e.g. author, editor). yield a new `dict`s for each such
	creator (subsequently referred to as simply "author").

	The resulting author `dict` will contain these keys:

	* `_aata_record_id`: The identifier of the corresponding article
	* `_aata_record_author_seq`: A integer identifying this author
	                             (unique within the scope of the article)
	* `label`: The name of the author
	* `creation_role`: The role the author played in the creation of the "article"
	                   (e.g. `'Author'`)
	* `names`: A `list` of names this organization may be identified by
	* `identifiers`: A `list` of (identifier, identifier type) pairs
	* `uid`: A unique ID for this organization
	* `parent`: The model object representing the corresponding article
	* `parent_data`: The `dict` representing the corresponding article
	* `events`: A `list` of `model.Creation` objects representing the part played by
	            the author in the article's creation event.
	'''
	lod_object = get_crom_object(data)
	event = model.Creation()
	lod_object.created_by = event

	authors = data.get('_authors', [])
	make_la_person = MakeLinkedArtPerson()
	for a in authors:
		make_la_person(a)
		person_name = a['label']
		person = get_crom_object(a)
		subevent = model.Creation()
		# TODO: The should really be asserted as object -created_by-> CreationEvent -part-> SubEvent
		# however, right now that assertion would get lost as it's data that belongs to the object,
		# and we're on the author's chain in the bonobo graph; object serialization has already happened.
		# we need to serialize the object's relationship to the creation event, and let it get merged
		# with the rest of the object's data.
		event.part = subevent
		role = a.get('creation_role')
		if role is not None:
			subevent._label = f'Creation sub-event for {role} by “{person_name}”'
		subevent.carried_out_by = person
	yield data

# article abstract chain

class MakeAATAAbstract(Configurable):
	helper = Option(required=True)
	language_code_map = Service('language_code_map')

	def __call__(self, data, language_code_map):
		'''
		Given a `dict` representing an "article," extract the abstract records.
		yield a new `dict`s for each such record.

		The resulting asbtract `dict` will contain these keys:

		* `_LOD_OBJECT`: A `model.LinguisticObject` object representing the abstract
		* `_aata_record_id`: The identifier of the corresponding article
		* `_aata_record_author_seq`: A integer identifying this abstract
									 (unique within the scope of the article)
		* `content`: The text content of the abstract
		* `language`: A model object representing the declared langauge of the abstract (if any)
		* `author_abstract_flag`: A boolean value indicating whether the article's authors also
								  authored the abstract
		* `identifiers`: A `list` of (identifier, identifier type) pairs
		* `_authors`: The authorship information from the input article `dict`
		* `uid`: A unique ID for this abstract
		* `parent`: The model object representing the corresponding article
		* `parent_data`: The `dict` representing the corresponding article
		'''
		lod_object = get_crom_object(data)
		for a in data.get('_abstracts', []):
			abstract_dict = {k: v for k, v in a.items() if k not in ('language',)}
			abstract_uri = self.helper.make_proj_uri('Abstract', data['_aata_record_id'], a['_aata_record_abstract_seq'])
			content = a.get('content')
			abstract = vocab.Abstract(ident=abstract_uri, content=content)
			abstract.refers_to = lod_object
			langcode = a.get('language')
			if langcode is not None:
				language = language_object_from_code(langcode, language_code_map)
				if language is not None:
					abstract.language = language
					abstract_dict['language'] = language

			if '_authors' in data:
				abstract_dict['_authors'] = data['_authors']

			# create a uid based on the AATA record id, the sequence number of the abstract
			# in that record, and which author we're handling right now
			abstract_dict.update({
				'parent_data': data,
				'uri': abstract_uri,
			})
			add_crom_data(data=abstract_dict, what=abstract)
			yield abstract_dict

def make_issue(data: dict):
	parent_data = data['parent_data']
	data['part_of'] = [parent_data] # the issue is a part of the journal

	volume_number = data.get('volume')
	if volume_number is not None:
		volume_data = parent_data.get('volumes', {}).get(volume_number)
		volume = get_crom_object(volume_data)
		if volume:
			data['part_of'].append(volume_data) # the issue is a part of the volume


	return data

def filter_abstract_authors(data: dict):
	'''Yield only those passed `dict` values for which the `'author_abstract_flag'` key is True.'''
	if 'author_abstract_flag' in data and data['author_abstract_flag']:
		yield data

# AATA Pipeline class

class AATAPipeline(PipelineBase):
	'''Bonobo-based pipeline for transforming AATA data from XML into JSON-LD.'''
	def __init__(self, input_path, abstracts_pattern, journals_pattern, series_pattern, people_pattern, corp_pattern, geog_pattern, subject_pattern, **kwargs):
		project_name = 'aata'
		self.input_path = input_path
		self.services = None

		helper = AATAUtilityHelper(project_name)

		super().__init__(project_name, helper=helper)

		vocab.register_vocab_class('IllustruationStatement', {'parent': model.LinguisticObject, 'id': '300015578', 'label': 'Illustruation Statement', 'metatype': 'brief text'})
		vocab.register_vocab_class('VolumeNumber', {'parent': model.Identifier, 'id': '300265632', 'label': 'Volume'})
		vocab.register_vocab_class('IssueNumber', {'parent': model.Identifier, 'id': '300312349', 'label': 'Issue'})
		vocab.register_vocab_class('PublicationPeriodNote', {'parent': model.LinguisticObject, 'id': '300081446', 'label': 'Publication Period Note', 'metatype': 'brief text'})

		self.graph = None
		self.models = kwargs.get('models', {})
		self.abstracts_pattern = abstracts_pattern
		self.journals_pattern = journals_pattern
		self.series_pattern = series_pattern
		self.people_pattern = people_pattern
		self.corp_pattern = corp_pattern
		self.geog_pattern = geog_pattern
		self.subject_pattern = subject_pattern

		self.limit = kwargs.get('limit')
		self.debug = kwargs.get('debug', False)
		self.pipeline_project_service_files_path = kwargs.get('pipeline_project_service_files_path', settings.pipeline_project_service_files_path)
		self.pipeline_common_service_files_path = kwargs.get('pipeline_common_service_files_path', settings.pipeline_common_service_files_path)

	def add_people_chain(self, graph, records, serialize=True):
		people = graph.add_chain(
			ExtractKeyedValue(key='record'),
			ModelPerson(helper=self.helper),
			_input=records.output
		)

		if serialize:
			_ = self.add_person_or_group_chain(graph, people, serialize=True)
		return people

	def add_journals_chain(self, graph, records, serialize=True):
		journals = graph.add_chain(
			ExtractKeyedValue(key='record'),
			ModelJournal(helper=self.helper),
			_input=records.output
		)

		activities = graph.add_chain(ExtractKeyedValues(key='_activities'), _input=journals.output)
		texts = graph.add_chain(ExtractKeyedValues(key='_texts'), _input=journals.output)

		if serialize:
			self.add_serialization_chain(graph, activities.output, model=self.models['Activity'])
			self.add_serialization_chain(graph, texts.output, model=self.models['LinguisticObject'])
			self.add_serialization_chain(graph, journals.output, model=self.models['LinguisticObject'])
		return journals

	def add_series_chain(self, graph, records, serialize=True):
		series = graph.add_chain(
			ExtractKeyedValue(key='record'),
			ModelSeries(helper=self.helper),
			_input=records.output
		)

		activities = graph.add_chain(ExtractKeyedValues(key='_activities'), _input=series.output)
		texts = graph.add_chain(ExtractKeyedValues(key='_texts'), _input=series.output)

		if serialize:
			self.add_serialization_chain(graph, activities.output, model=self.models['Activity'])
			self.add_serialization_chain(graph, texts.output, model=self.models['LinguisticObject'])
			self.add_serialization_chain(graph, series.output, model=self.models['LinguisticObject'])
		return series

	def add_corp_chain(self, graph, records, serialize=True):
		corps = graph.add_chain(
			ExtractKeyedValue(key='record'),
			ModelCorp(helper=self.helper),
			_input=records.output
		)

		activities = graph.add_chain(ExtractKeyedValues(key='_activities'), _input=corps.output)

		if serialize:
			self.add_places_chain(graph, corps, key='_places', serialize=True)
			self.add_serialization_chain(graph, activities.output, model=self.models['Activity'])
			self.add_serialization_chain(graph, corps.output, model=self.models['Group'])
		return people

# 	def add_subject_chain(self, graph, records, serialize=True):
# 		subjects = graph.add_chain(
# 			ExtractKeyedValue(key='record'),
# 			ModelSubject(helper=self.helper),
# 			_input=records.output
# 		)
#
# 		if serialize:
# 			self.add_serialization_chain(graph, subjects.output, model=self.models['XXX'])
# 		return people

	def add_geog_chain(self, graph, records, serialize=True):
		places = graph.add_chain(
			ExtractKeyedValue(key='record'),
			ModelPlace(helper=self.helper),
			_input=records.output
		)

		activities = graph.add_chain(ExtractKeyedValues(key='_activities'), _input=places.output)

		if serialize:
			self.add_serialization_chain(graph, activities.output, model=self.models['Activity'])
			self.add_places_chain(graph, places, key=None, serialize=True)
			self.add_serialization_chain(graph, places.output, model=self.models['Place'])
		return people

	def add_articles_chain(self, graph, records, serialize=True):
		'''Add transformation of article records to the bonobo pipeline.'''
		articles = graph.add_chain(
			ExtractKeyedValue(key='record'),
			ModelArticle(helper=self.helper),
			_input=records.output
		)

		people = graph.add_chain(ExtractKeyedValues(key='_people'), _input=articles.output)
		activities = graph.add_chain(ExtractKeyedValues(key='_activities'), _input=articles.output)
		groups = graph.add_chain(ExtractKeyedValues(key='_groups'), _input=articles.output)

		if serialize:
			# write ARTICLES data
			self.add_places_chain(graph, articles, key='_places', serialize=True)
			self.add_serialization_chain(graph, articles.output, model=self.models['LinguisticObject'])
			self.add_serialization_chain(graph, groups.output, model=self.models['Group'])
			self.add_serialization_chain(graph, activities.output, model=self.models['Activity'])
			_ = self.add_person_or_group_chain(graph, people, serialize=True)
		return articles

	def _add_abstracts_graph(self, graph):
		abstract_records = graph.add_chain(
			MatchingFiles(path='/', pattern=self.abstracts_pattern, fs='fs.data.aata'),
			CurriedXMLReader(xpath='/AATA_XML/record', fs='fs.data.aata', limit=self.limit),
			_xml_element_to_dict,
		)
		articles = self.add_articles_chain(graph, abstract_records)
		return articles

	def _add_people_graph(self, graph):
		records = graph.add_chain(
			MatchingFiles(path='/', pattern=self.people_pattern, fs='fs.data.aata'),
			CurriedXMLReader(xpath='/auth_person_XML/record', fs='fs.data.aata', limit=self.limit),
			_xml_element_to_dict,
		)
		people = self.add_people_chain(graph, records)
		return people

	def _add_journals_graph(self, graph):
		records = graph.add_chain(
			MatchingFiles(path='/', pattern=self.journals_pattern, fs='fs.data.aata'),
			CurriedXMLReader(xpath='/journal_XML/record', fs='fs.data.aata', limit=self.limit),
			_xml_element_to_dict,
		)
		journals = self.add_journals_chain(graph, records)
		return journals

	def _add_series_graph(self, graph):
		records = graph.add_chain(
			MatchingFiles(path='/', pattern=self.series_pattern, fs='fs.data.aata'),
			CurriedXMLReader(xpath='/series_XML/record', fs='fs.data.aata', limit=self.limit),
			_xml_element_to_dict,
		)
		series = self.add_series_chain(graph, records)
		return series

	def _add_corp_graph(self, graph):
		records = graph.add_chain(
			MatchingFiles(path='/', pattern=self.corp_pattern, fs='fs.data.aata'),
			CurriedXMLReader(xpath='/auth_corp_XML/record', fs='fs.data.aata', limit=self.limit),
			_xml_element_to_dict,
		)
		corps = self.add_corp_chain(graph, records)
		return corps

	def _add_geog_graph(self, graph):
		records = graph.add_chain(
			MatchingFiles(path='/', pattern=self.geog_pattern, fs='fs.data.aata'),
			CurriedXMLReader(xpath='/auth_geog_XML/record', fs='fs.data.aata', limit=self.limit),
			_xml_element_to_dict,
		)
		geog = self.add_geog_chain(graph, records)
		return geog

# 	def _add_subject_graph(self, graph):
# 		records = graph.add_chain(
# 			MatchingFiles(path='/', pattern=self.subject_pattern, fs='fs.data.aata'),
# 			CurriedXMLReader(xpath='/auth_subject_XML/record', fs='fs.data.aata', limit=self.limit),
# 			_xml_element_to_dict,
# 		)
# 		subject = self.add_subject_chain(graph, records)
# 		return subject

	def _construct_graph(self):
		graph = bonobo.Graph()

		articles = self._add_abstracts_graph(graph)
		journals = self._add_journals_graph(graph)
		series = self._add_series_graph(graph)
		people = self._add_people_graph(graph)
		corps	= self._add_corp_graph(graph)
		places	= self._add_geog_graph(graph)
# 		subjects = self._add_subject_graph(graph)

		self.graph = graph
		return graph

	def get_graph(self, **kwargs):
		'''Construct the bonobo pipeline to fully transform AATA data from XML to JSON-LD.'''
		if not self.graph:
			self._construct_graph()
		return self.graph

	def run(self, **options):
		'''Run the AATA bonobo pipeline.'''
		print(f"- Limiting to {self.limit} records per file", file=sys.stderr)
		services = self.get_services(**options)
		graph = self.get_graph(**options, services=services)
		self.run_graph(graph, services=services)

		print('Serializing static instances...', file=sys.stderr)
		for model_name, instances in self.static_instances.used_instances().items():
			g = bonobo.Graph()
			nodes = self.serializer_nodes_for_model(model=self.models[model_name])
			values = instances.values()
			source = g.add_chain(GraphListSource(values))
			self.add_serialization_chain(g, source.output, model=self.models[model_name])
			self.run_graph(g, services={})

class AATAFilePipeline(AATAPipeline):
	'''
	AATA pipeline with serialization to files based on Arches model and resource UUID.

	If in `debug` mode, JSON serialization will use pretty-printing. Otherwise,
	serialization will be compact.
	'''
	def __init__(self, input_path, abstracts_pattern, journals_pattern, series_pattern, people_pattern, corp_pattern, geog_pattern, subject_pattern, **kwargs):
		super().__init__(input_path, abstracts_pattern, journals_pattern, series_pattern, people_pattern, corp_pattern, geog_pattern, subject_pattern, **kwargs)
		self.use_single_serializer = False
		self.output_path = kwargs.get('output_path')

	def serializer_nodes_for_model(self, model=None):
		nodes = []
		if self.debug:
			nodes.append(MergingFileWriter(directory=self.output_path, partition_directories=True, compact=False, model=model))
		else:
			nodes.append(MergingFileWriter(directory=self.output_path, partition_directories=True, compact=True, model=model))
		return nodes
