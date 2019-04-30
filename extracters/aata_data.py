'''
Classes and utiltiy functions for instantiating, configuring, and
running a bonobo pipeline for converting AATA XML data into JSON-LD.
'''

# AATA Extracters

import sys
import pprint
import itertools

import iso639
import bonobo
from bonobo.config import Configurable, Option
from bonobo.constants import NOT_MODIFIED
from bonobo.nodes import Limit
import lxml.etree
from sqlalchemy import create_engine
from langdetect import detect

import settings
from cromulent import model, vocab
from .cleaners import date_cleaner
from .linkedart import \
			MakeLinkedArtAbstract, \
			MakeLinkedArtLinguisticObject, \
			MakeLinkedArtOrganization
from .knoedler_linkedart import make_la_person
from .xml import XMLReader
from .basic import \
			add_uuid, \
			AddArchesModel, \
			Serializer

localIdentifier = None # TODO: aat:LocalIdentifier?
legacyIdentifier = None # TODO: aat:LegacyIdentifier?

# utility functions

def language_object_from_code(code):
	'''
	Given a three-letter language (ISO639-2) code, return a model.Language object for the
	corresponding language.

	For example, `language_object_from_code('eng')` returns an object representing
	the English language.
	'''
	languages = {
		# TODO: there must be a better way to do this than keep a static mapping of languages
		'eng': {'ident': 'http://vocab.getty.edu/aat/300388277', 'label': 'English'},
		'fra': {'ident': 'http://vocab.getty.edu/aat/300388306', 'label': 'French'},
		'fre': {'ident': 'http://vocab.getty.edu/aat/300388306', 'label': 'French'},
		'ger': {'ident': 'http://vocab.getty.edu/aat/300388344', 'label': 'German'},
		'deu': {'ident': 'http://vocab.getty.edu/aat/300388344', 'label': 'German'},
		'pol': {'ident': 'http://vocab.getty.edu/aat/300389109', 'label': 'Polish'},
		'ita': {'ident': 'http://vocab.getty.edu/aat/300388474', 'label': 'Italian'},
		'nld': {'ident': 'http://vocab.getty.edu/aat/300388256', 'label': 'Dutch'},
		'swe': {'ident': 'http://vocab.getty.edu/aat/300389336', 'label': 'Swedish'}
	}
	try:
		kwargs = languages[code]
		return model.Language(**kwargs)
	except KeyError:
		print('*** No AAT link for language %r' % (code,))
	except Exception as e:
		print('*** language_object_from_code: %s' % (e,))
		raise e

# main article chain

def make_aata_article_dict(e):
	'''
	Given an XML element representing an AATA record, extract information about the
	"article" (this might be a book, chapter, journal article, etc.) including:

	* document type
	* titles and title translations
	* organizations and their role (e.g. publisher)
	* creators and thier role (e.g. author, editor)
	* abstracts
	* languages

	This information is returned in a single `dict`.
	'''
	doc_type = e.findtext('./record_desc_group/doc_type')
	title = e.findtext('./title_group[title_type = "Analytic"]/title')
	translations = list([t.text for t in
		e.xpath('./title_group[title_type = "Analytic"]/title_translated')])
	languages = set([t.text for t in e.xpath('./notes_group/lang_doc|./notes_group/lang_summary')])
	aata_id = e.findtext('./record_id_group/record_id')
	organizations = list(_xml_extract_organizations(e, aata_id))
	authors = list(_xml_extract_authors(e, aata_id))
	abstracts = list(_xml_extract_abstracts(e, aata_id))
	uid = 'AATA-%s-%s-%s' % (doc_type, aata_id, title)

	return {
		'_source_element': e,
		'label': title,
		'languages': languages,
		'_document_type': e.findtext('./record_desc_group/doc_type'),
		'_organizations': list(organizations),
		'_authors': list(authors),
		'_abstracts': list(abstracts),
		'_aata_record_id': aata_id,
		'translations': list(translations),
		'uid': uid
	}

def _xml_extract_abstracts(e, aata_id):
	'''Extract information about abstracts from an "article" record XML element'''
	rids = [e.text for e in e.findall('./record_id_group/record_id')]
	lids = [e.text for e in e.findall('./record_id_group/legacy_id')]
	for i, ag in enumerate(e.xpath('./abstract_group')):
		a = ag.find('./abstract')
		author_abstract_flag = ag.findtext('./author_abstract_flag')
		if a is not None:
			content = a.text
			language = a.attrib.get('lang')

			localIds = [(i, localIdentifier) for i in rids]
			legacyIds = [(i, legacyIdentifier) for i in lids]
			yield {
				'_aata_record_id': aata_id,
				'_aata_record_abstract_seq': i,
				'content': content,
				'language': language,
				'author_abstract_flag': (author_abstract_flag == 'yes'),
				'identifiers': localIds + legacyIds,
			}

def _xml_extract_organizations(e, aata_id):
	'''Extract information about organizations from an "article" record XML element'''
	i = -1
	for ig in e.xpath('./imprint_group/related_organization'):
		role = ig.findtext('organization_type')
		properties = {}
		for pair in ig.xpath('./additional_org_info'):
			key = pair.findtext('label')
			value = pair.findtext('value')
			properties[key] = value
		for o in ig.xpath('./organization'):
			i += 1
			aid = o.find('./organization_id')
			if aid is not None:
				name = aid.findtext('display_term')
				auth_id = aid.findtext('gaia_auth_id')
				auth_type = aid.findtext('gaia_auth_type')
				yield {
					'_aata_record_id': aata_id,
					'_aata_record_organization_seq': i,
					'label': name,
					'role': role,
					'properties': properties,
					'names': [(name,)],
					'identifiers': [(auth_id, localIdentifier)],
					'uid': 'AATA-Org-%s-%s-%s' % (auth_type, auth_id, name)
				}
			else:
				print('*** No organization_id found for record %s:' % (o,))
				print(lxml.etree.tostring(o).decode('utf-8'))

def _xml_extract_authors(e, aata_id):
	'''Extract information about authors from an "article" record XML element'''
	i = -1
	for ag in e.xpath('./authorship_group'):
		role = ag.findtext('author_role')
		for a in ag.xpath('./author'):
			i += 1
			aid = a.find('./author_id')
			if aid is not None:
				name = aid.findtext('display_term')
				auth_id = aid.findtext('gaia_auth_id')
				auth_type = aid.findtext('gaia_auth_type')
				author = {}
				if auth_id is None:
					print('*** no gaia auth id for author in record %r' % (aata_id,))
					uid = 'AATA-P-Internal-%s-%d' % (aata_id, i)
				else:
					uid = 'AATA-P-%s-%s-%s' % (auth_type, auth_id, name)

				if role is not None:
					author['creation_role'] = role
				else:
					print('*** No author role found for authorship group')
					print(lxml.etree.tostring(ag).decode('utf-8'))

				author.update({
					'_aata_record_id': aata_id,
					'_aata_record_author_seq': i,
					'label': name,
					'names': [(name,)],
					'identifiers': [(auth_id, localIdentifier)],
					'uid': uid
				})
				yield author
			else:
				sys.stderr.write('*** No author_id found for record %s\n' % (aata_id,))
# 				sys.stderr.write(lxml.etree.tostring(a).decode('utf-8'))
# 				sys.stderr.write('\n')

def add_aata_object_type(data):
	'''
	Given an "article" `dict` containing a `_document_type` key which has a two-letter
	document type string (e.g. 'JA' for journal article, 'BC' for book), add a new key
	`object_type` containing the corresponding `vocab` class. This class can be used to
	construct a model object for this "article".

	For example, `add_aata_object_type({'_document_type': 'AV', ...})` returns the `dict`:
	`{'_document_type': 'AV', 'document_type': vocab.AudioVisualContent, ...}`.
	'''
	doc_types = { # TODO: should this be in settings (or elsewhere)?
		'AV': vocab.AudioVisualContent,
		'BA': vocab.Chapter,
		'BC': vocab.Monograph, # TODO: is this right for a "Book - Collective"?
		'BM': vocab.Monograph,
		'JA': vocab.Article,
		'JW': vocab.Issue,
		'PA': vocab.Patent,
		'TH': vocab.Thesis,
		'TR': vocab.TechnicalReport
	}
	atype = data['_document_type']
	data['object_type'] = doc_types[atype]
	return data

# imprint organizations chain (publishers, distributors)

def extract_imprint_orgs(data):
	'''
	Given a `dict` representing an "article," extract the "imprint organization" records
	and their role (e.g. publisher, distributor). yield a new `dict`s for each such
	organization.

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
	* `parent`: The model object representing the corresponding article
	* `parent_data`: The `dict` representing the corresponding article

	In addition, these keys may be present (if applicable):

	* `event_label`: A description of the activity the organization performed as part of
	                 the article's creation
	* `publication_date`: A string describing the date (range) of publication
	'''
	lod_object = data['_LOD_OBJECT']
	organizations = data['_organizations']
	for o in organizations:
		org = {k: v for k, v in o.items()}

		properties = o.get('properties')
		role = o.get('role')
		if role is not None:
			activity_names = {
				'Distributor': 'Distributing',
				'Publisher': 'Publishing',
				# TODO: Need to also handle roles: Organization, Sponsor, University
			}
			if role in activity_names:
				org['event_label'] = activity_names[role]
			else:
				print('*** No/unknown organization role (%r) found for imprint_group in %s:' % (
					role, lod_object,))
				pprint.pprint(o)

			if role == 'Publisher' and 'DatesOfPublication' in properties:
				pubdate = properties['DatesOfPublication']
				org['publication_date'] = pubdate

		org.update({
			'parent': lod_object,
			'parent_data': data,
		})
		yield org

class CleanDateToSpan(Configurable):
	'''
	Supplied with a key name, attempt to parse the value in `input[key]`` as a date or
	date range, and create a new `TimeSpan` object for the parsed date(s). Store the
	resulting timespan in `input[key + '_span']`.
	'''

	key = Option(str, required=True)
	optional = Option(bool, default=True)

	@staticmethod
	def string_to_span(value):
		'''Parse a string value and attempt to create a corresponding `model.TimeSpan` object.'''
		try:
			date_from, date_to = date_cleaner(value)
			ts = model.TimeSpan()
			if date_from is not None:
				ts.begin_of_the_begin = date_from.strftime("%Y-%m-%dT%H:%M:%SZ")
			if date_to is not None:
				ts.end_of_the_end = date_to.strftime("%Y-%m-%dT%H:%M:%SZ")
			return ts
		except:
			print('*** Unknown date format: %r' % (value,))
			return None

	def __call__(self, data, *args, **kwargs):
		if self.key in data:
			value = data[self.key]
			ts = self.string_to_span(value)
			if ts is not None:
				data['%s_span' % self.key] = ts
				return data
		else:
			if not self.optional:
				print('*** key %r is not in the data object:' % (self.key,))
				pprint.pprint(data)
		return NOT_MODIFIED

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
	lod_object = o['parent']
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

def make_aata_authors(data):
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
	lod_object = data['_LOD_OBJECT']
	event = model.Creation()
	lod_object.created_by = event
	event.created = lod_object

	authors = data.get('_authors', [])
	for a in authors:
		subevent = model.Creation()
		event.part = subevent
		subevent.part_of = event
		role = a.get('creation_role')
		if role is not None:
			subevent._label = 'Creation sub-event for %s' % (role,)
		author = {k: v for k, v in a.items()}
		author.update({
			'parent': lod_object,
			'parent_data': data,
			'events': [subevent],
		})
		yield author

# article abstract chain

def detect_title_language(data: dict):
	'''
	Given a `dict` representing a Linguistic Object, attempt to detect the language of
	the value for the `label` key.  If 
	'''
	languages = data.get('languages', set())
	try:
		title = data['label']
		if title is None:
			return NOT_MODIFIED
		translations = data.get('translations', [])
		if len(translations) and len(languages):
			detected = detect(title)
			threealpha = iso639.to_iso639_2(detected)
			if threealpha in languages:
				lang = language_object_from_code(threealpha)
				if lang is not None:
					# we have confidence that we've matched the language of the title
					# because it is one of the declared languages for the record
					# document/summary
					data['label'] = (title, lang)
			else:
				# the detected language of the title was not declared in the record data,
				# so we lack confidence to proceed
				pass
	except iso639.NonExistentLanguageError as e:
		sys.stderr.write('*** Unrecognized language code detected: %r\n' % (detected,))
	except KeyError as e:
		sys.stderr.write(
			'*** LANGUAGE: detected but unrecognized title language %r '
			'(cf. declared in metadata: %r): %s\n' % (e.args[0], languages, title)
		)
	except Exception as e:
		print('*** detect_title_language error: %r' % (e,))
	return NOT_MODIFIED

def make_aata_abstract(data):
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
	lod_object = data['_LOD_OBJECT']
	for a in data.get('_abstracts', []):
		abstract = model.LinguisticObject()
		abstract_dict = {k: v for k, v in a.items() if k not in ('language',)}

		abstract.content = a.get('content')
		abstract.classified_as = model.Type(
			ident='http://vocab.getty.edu/aat/300026032',
			label='Abstract' # TODO: is this the right aat URI?
		)
		abstract.refers_to = lod_object
		langcode = a.get('language')
		if langcode is not None:
			l = language_object_from_code(langcode)
			if l is not None:
				abstract.language = l
				abstract_dict['language'] = l

		if '_authors' in data:
			abstract_dict['_authors'] = data['_authors']

		# create a uid based on the AATA record id, the sequence number of the abstract
		# in that record, and which author we're handling right now
		uid = 'AATA-Abstract-%s-%d' % (data['_aata_record_id'], a['_aata_record_abstract_seq'])
		abstract_dict.update({
			'_LOD_OBJECT': abstract,
			'parent': lod_object,
			'parent_data': data,
			'uid': uid
		})
		yield abstract_dict

def filter_abstract_authors(data: dict):
	'''Yield only those passed `dict` values for which the `'author_abstract_flag'` key is True.'''
	if 'author_abstract_flag' in data and data['author_abstract_flag']:
		yield data

class AddDataDependentArchesModel(Configurable):
	'''
	Set the `_ARCHES_MODEL` key in the supplied `dict` to the appropriate arches model UUID
	and return it.
	'''
	models = Option()
	def __call__(self, data, *args, **kwargs):
		data['_ARCHES_MODEL'] = self.models['LinguisticObject']
		return data

# AATA Pipeline class

class AATAPipeline:
	'''Bonobo-based pipeline for transforming AATA data from XML into JSON-LD.'''
	def __init__(self, input_path, files, **kwargs):
		self.models = kwargs.get('models', {})
		self.files = files
		self.limit = kwargs.get('limit')
		self.debug = kwargs.get('debug')
		self.input_path = input_path
		if self.debug:
			self.files = [self.files[0]]
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
			'fs.data.aata': bonobo.open_fs(self.input_path)
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

	def add_articles_chain(self, graph, records, serialize=True):
		'''Add transformation of article records to the bonobo pipeline.'''
		if self.limit is not None:
			records = graph.add_chain(
				Limit(self.limit),
				_input=records.output
			)
		articles = graph.add_chain(
			make_aata_article_dict,
			add_uuid,
			add_aata_object_type,
			detect_title_language,
			MakeLinkedArtLinguisticObject(),
			AddDataDependentArchesModel(models=self.models),
			_input=records.output
		)
		if serialize:
			# write ARTICLES data
			self.add_serialization_chain(graph, articles.output)
		return articles

	def add_people_chain(self, graph, articles, serialize=True):
		'''Add transformation of author records to the bonobo pipeline.'''
		model_id = self.models.get('Person', 'XXX-Person-Model')
		people = graph.add_chain(
			make_aata_authors,
			AddArchesModel(model=model_id),
			add_uuid,
			make_la_person,
			_input=articles.output
		)
		if serialize:
			# write PEOPLE data
			self.add_serialization_chain(graph, people.output)
		return people

	def add_abstracts_chain(self, graph, articles, serialize=True):
		'''Add transformation of abstract records to the bonobo pipeline.'''
		model_id = self.models.get('LinguisticObject', 'XXX-LinguisticObject-Model')
		abstracts = graph.add_chain(
			make_aata_abstract,
			AddArchesModel(model=model_id),
			add_uuid,
			MakeLinkedArtAbstract(),
			_input=articles.output
		)

		# for each author of an abstract...
		author_abstracts = graph.add_chain(
			filter_abstract_authors,
			_input=abstracts.output
		)
		self.add_people_chain(graph, author_abstracts)

		if serialize:
			# write ABSTRACTS data
			self.add_serialization_chain(graph, abstracts.output)
		return abstracts

	def add_organizations_chain(self, graph, articles, serialize=True):
		'''Add transformation of organization records to the bonobo pipeline.'''
		model_id = self.models.get('Organization', 'XXX-Organization-Model')
		organizations = graph.add_chain(
			extract_imprint_orgs,
			CleanDateToSpan(key='publication_date'),
			make_aata_org_event,
			AddArchesModel(model=model_id), # TODO: model for organizations?
			add_uuid,
			MakeLinkedArtOrganization(),
			_input=articles.output
		)
		if serialize:
			# write ORGANIZATIONS data
			self.add_serialization_chain(graph, organizations.output)
		return organizations

	def get_graph(self):
		'''Construct the bonobo pipeline to fully transform AATA data from XML to JSON-LD.'''
		graph = bonobo.Graph()
		files = self.files[:]
		if self.debug:
			sys.stderr.write("Processing %s\n" % (files[0],))

		for f in files:
			records = graph.add_chain(
				XMLReader(f, xpath='/AATA_XML/record', fs='fs.data.aata')
			)
			articles = self.add_articles_chain(graph, records)
			self.add_people_chain(graph, articles)
			self.add_abstracts_chain(graph, articles)
			self.add_organizations_chain(graph, articles)

		return graph

	def run(self, **options):
		'''Run the AATA bonobo pipeline.'''
		sys.stderr.write("- Limiting to %d records per file\n" % (self.limit,))
		sys.stderr.write("- Using serializer: %r\n" % (self.serializer,))
		sys.stderr.write("- Using writer: %r\n" % (self.writer,))
		graph = self.get_graph(**options)
		services = self.get_services(**options)
		bonobo.run(
			graph,
			services=services
		)
