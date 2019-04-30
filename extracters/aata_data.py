# TODO: move cromulent model code out of this module

# AATA Extracters

import sys
import pprint
import itertools

import bonobo
from bonobo.config import Configurable, Option
from bonobo.constants import NOT_MODIFIED
from bonobo.nodes import Limit
import lxml.etree
from sqlalchemy import create_engine

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
	languages = {
		'eng': {'ident': 'http://vocab.getty.edu/aat/300388277', 'label': 'English'}
	}
	try:
		kwargs = languages[code]
		return model.Language(**kwargs)
	except KeyError:
		print('*** No AAT link for language %r' % (code,))

# main article chain

def make_aata_article_dict(e):
	doc_type = e.findtext('./record_desc_group/doc_type')
	title = e.findtext('./title_group[title_type = "Analytic"]/title')
	translations = list([t.text for t in
		e.xpath('./title_group[title_type = "Analytic"]/title_translated')])
	aata_id = e.findtext('./record_id_group/record_id')
	organizations = list(_xml_extract_organizations(e, aata_id))
	authors = list(_xml_extract_authors(e, aata_id))
	abstracts = list(_xml_extract_abstracts(e, aata_id))
	uid = 'AATA-%s-%s-%s' % (doc_type, aata_id, title)

	return {
		'_source_element': e,
		'label': title,
		'_document_type': e.findtext('./record_desc_group/doc_type'),
		'_organizations': list(organizations),
		'_authors': list(authors),
		'_abstracts': list(abstracts),
		'_aata_record_id': aata_id,
		'translations': list(translations),
		'uid': uid
	}

def _xml_extract_abstracts(e, aata_id):
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
	doc_types = { # should this be in settings (or elsewhere)?
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

	def __call__(self, data):
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

def make_aata_imprint_orgs(o: dict):
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

def make_aata_abstract(data):
	lod_object = data['_LOD_OBJECT']
	for a in data.get('_abstracts', []):
		abstract = model.LinguisticObject()
		abstract_dict = {k: v for k, v in a.items()}

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

		abstract_dict = {k: v for k, v in a.items()}
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
	if 'author_abstract_flag' in data and data['author_abstract_flag']:
		yield data

class AddDataDependentArchesModel(Configurable):
	models = Option()
	def __call__(self, data):
		data['_ARCHES_MODEL'] = self.models['LinguisticObject']
		return data

# AATA Pipeline class

class AATAPipeline:
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
		return {
			'trace_counter': itertools.count(),
			'gpi': create_engine(settings.gpi_engine),
			'aat': create_engine(settings.aat_engine),
			'uuid_cache': create_engine(settings.uuid_cache_engine),
			'fs.data.aata': bonobo.open_fs(self.input_path)
		}

	def add_serialization_chain(self, graph, input_node):
		if self.writer is not None:
			graph.add_chain(
				self.serializer,
				self.writer,
				_input=input_node
			)
		else:
			sys.stderr.write('*** No serialization chain defined\n')

	def add_articles_chain(self, graph, records, serialize=True):
		if self.limit is not None:
			records = graph.add_chain(
				Limit(self.limit),
				_input=records.output
			)
		articles = graph.add_chain(
			make_aata_article_dict,
			add_uuid,
			add_aata_object_type,
			MakeLinkedArtLinguisticObject(),
			AddDataDependentArchesModel(models=self.models),
			_input=records.output
		)
		if serialize:
			# write ARTICLES data
			self.add_serialization_chain(graph, articles.output)
		return articles

	def add_people_chain(self, graph, articles, serialize=True):
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
		model_id = self.models.get('Organization', 'XXX-Organization-Model')
		organizations = graph.add_chain(
			extract_imprint_orgs,
			CleanDateToSpan(key='publication_date'),
			make_aata_imprint_orgs,
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
		sys.stderr.write("- Limiting to %d records per file\n" % (self.limit,))
		sys.stderr.write("- Using serializer: %r\n" % (self.serializer,))
		sys.stderr.write("- Using writer: %r\n" % (self.writer,))
		graph = self.get_graph(**options)
		services = self.get_services(**options)
		bonobo.run(
			graph,
			services=services
		)
