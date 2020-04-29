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
from pipeline.util import identity, GraphListSource, ExtractKeyedValue, ExtractKeyedValues, MatchingFiles, timespan_from_outer_bounds
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

	def article_uri(self, a_id):
		return self.make_proj_uri('Article', a_id)

	def corporate_body_uri(self, corp_id):
		return self.make_proj_uri('CB', corp_id)

	def person_uri(self, p_id):
		return self.make_proj_uri('Person', p_id)

	def place_uri(self, geog_id):
		return self.make_proj_uri('Place', geog_id)

# class MakeAATAJournalDict(Configurable):
# 	helper = Option(required=True)
# 	
# 	def __call__(self, e):
# 		record = _xml_element_to_dict(e)['record']
# 		data = _extract_journal(record, self.helper)
# 		return data
# 
# class MakeAATASeriesDict(Configurable):
# 	helper = Option(required=True)
# 	
# 	def __call__(self, e):
# 		record = _xml_element_to_dict(e)['record']
# 		data = _extract_series(record, self.helper)
# 	# 	print(f'SERIES: {pprint.pformat(data)}')
# 		return data
# 
# class MakePublishingActivity(Configurable):
# 	helper = Option(default=True)
# 	
# 	def __call__(self, data:dict):
# 		lo = get_crom_object(data)
# 		components = data['uri_components']
# 		pubs = data.get('publishers', [])
# 		title = data.get('label')
# 	
# 		previous_components = data.get('previous', {}).get('uri_components')
# 		for pub in pubs:
# 			start, end = data['years']
# 			uri = self.helper.make_proj_uri(*components, 'Publishing')
# 			event = vocab.Publishing(ident=uri, label=f'Publishing event for “{title}”')
# 			try:
# 				event.timespan = timespan_from_outer_bounds(start, end, inclusive=True)
# 			except:
# 				pass
# 			lo.used_for = event
# 			if previous_components:
# 				# TODO: handle modeling of history_reason field
# 				previous_uri = self.helper.make_proj_uri(*previous_components, 'Publishing')
# 	# 			print(f'*** {uri} ===[continued]==> {previous_uri}')
# 				event.continued = vocab.Publishing(ident=previous_uri)
# 
# 			if 'events' not in pub:
# 				pub['events'] = []
# 			pub['events'].append(event)
# 		return data

def _xml_element_to_dict(e):
	chunk = lxml.etree.tostring(e).decode('utf-8')
	data = xmltodict.parse(chunk)
	s = json.dumps(data)
	data = json.loads(s)
	return data

# class MakeAATAArticleDict_old(Configurable):
# 	helper = Option(required=True)
# 	language_code_map = Service('language_code_map')
# 	
# 	def __call__(self, e, language_code_map):
# 		'''
# 		Given an XML element representing an AATA record, extract information about the
# 		"article" (this might be a book, chapter, journal article, etc.) including:
# 
# 		* document type
# 		* titles and title translations
# 		* organizations and their role (e.g. publisher)
# 		* creators and thier role (e.g. author, editor)
# 		* abstracts
# 		* languages
# 
# 		This information is returned in a single `dict`.
# 		'''
# 
# 		record = _xml_element_to_dict(e)['record']
# 		data = _extract_article(record, language_code_map, self.helper)
# 		aata_id = record['record_id_group']['record_id']
# 		_extract_organizations(data, aata_id, self.helper)
# 		_extract_authors(data, aata_id, self.helper)
# 		_extract_abstracts(data, aata_id)
# 
# 		return data
# 
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
# 
# def _extract_sponsor_group(data, helper):
# 	aata_id = data['gaia_corp_id']
# 	name = data['sponsor_name']
# 	geog_id = data.get('gaia_geog_id')
# 	city = data.get('sponsor_city')
# 	country = data.get('sponsor_country')
# 	
# 	identifiers = []
# 	if geog_id:
# 		identifiers.append(vocab.LocalNumber(ident='', content=geog_id))
# 	return {
# 		'label': name,
# 		'uri': helper.make_proj_uri('Sponsor', aata_id),
# 		'_aata_record_id': aata_id,
# 		'identifiers': [(aata_id, vocab.LocalNumber(ident=''))],
# 		'place': {
# 			'label': city,
# 			'identifiers': identifiers,
# 			'type': 'City',
# 			'part_of': {
# 				'label': country,
# 				'type': 'Country',
# 			}
# 		}
# 	}
# 
# def _extract_publisher_group(data, helper):
# 	aata_id = data['gaia_corp_id']
# 	name = data['publisher_name']
# 	geog_id = data.get('gaia_geog_id')
# 	city = data.get('sponsor_city')
# 	country = data.get('sponsor_country')
# 
# 	identifiers = []
# 	if geog_id:
# 		identifiers.append(vocab.LocalNumber(ident='', content=geog_id))
# 	return {
# 		'label': name,
# 		'uri': helper.make_proj_uri('Publisher', aata_id),
# 		'_aata_record_id': aata_id,
# 		'identifiers': [(aata_id, vocab.LocalNumber(ident=''))],
# 		'place': {
# 			'label': city,
# 			'identifiers': identifiers,
# 			'type': 'City',
# 			'part_of': {
# 				'label': country,
# 				'type': 'Country',
# 			}
# 		}
# 	}
# 
# def _extract_journal(record, helper):
# 	'''Extract information about a journal record XML element'''
# 	aata_id = record['record_desc_group']['record_id']
# 	jg = record['journal_group']
# 	title = jg.get('title')
# 	if title:
# 		journal_label = f'“{title}”'
# 	else:
# 		journal_label = f'Journal'
# 	var_title_strings = [t for t in _as_list(jg.get('variant_title', []))]
# 	translations = [t for t in _as_list(jg.get('title_translated', []))]
# 
# # 	lang_name = e.findtext('./journal_group/language/lang_name')
# # 	lang_scope = e.findtext('./journal_group/language/lang_scope')
# 
# 	start_year = jg.get('start_year')
# 	cease_year = jg.get('cease_year')
# 	
# 	journal_uri = helper.make_proj_uri('Journal', aata_id)
# 	issn = [(i, vocab.IssnIdentifier(ident='')) for i in _as_list(jg.get('issn'))]
# 
# 	publishers = [_extract_publisher_group(pg, helper) for pg in _as_list(record.get('publisher_group'))]
# 	sponsors = [_extract_sponsor_group(sg, helper) for sg in _as_list(record.get('sponsor_group'))]
# 
# 	issues = []
# 	volumes = {}
# 	
# 	for ig in _as_list(record.get('issue_group')):
# 		issue_id = ig['issue_id']
# 		issue_title = ig.get('title')
# 		issue_translations = _as_list(ig.get('title_translated'))
# 		volume_number = ig.get('volume')
# 		issue_id = ig.get('issue_id')
# 		issue_number = ig.get('number')
# 		
# 		date = ig.get('date')
# 		notes = []
# 		if date:
# 			season = date.get('display_date')
# 			month = date.get('sort_month')
# 			year = date.get('sort_year')
# 			
# 			if year:
# 				begin = datetime.strptime(ymd_to_datetime(year, month, None, which='begin'), "%Y-%m-%dT%H:%M:%S")
# 				end = datetime.strptime(ymd_to_datetime(year, month, None, which='end'), "%Y-%m-%dT%H:%M:%S")
# 				ts = timespan_from_outer_bounds(begin, end, inclusive=True)
# 				# TODO: attach ts to the issue somehow
# 			elif month:
# 				print(f'*** Found an issue with sort_month but not sort_year (!?) in record {aata_id}')
# 
# 			if season:
# 				# TODO: should the PublicationPeriodNote be attached to the issue, or the
# 				#       publication date?
# 				notes.append(vocab.PublicationPeriodNote(ident='', content=season))
# 
# 		note = ig.get('note')
# 		if note:
# 			# TODO: is there a more specific Note type we can use for this?
# 			notes.append(note)
# 
# 		chron = ig.get('enum_chron')
# 		if chron:
# 			# TODO: is there a more specific Note type we can use for this?
# 			notes.append(vocab.Note(ident='', content=chron))
# 
# 		if volume_number:
# 			volume_uri = helper.make_proj_uri('Journal', aata_id, 'Volume', volume_number)
# 			volumes[volume_number] = {
# 				'uri': volume_uri,
# 				'object_type': vocab.VolumeText,
# 				'label': f'Volume {volume_number} of {journal_label}',
# 				'_aata_record_id': issue_id,
# 				'identifiers': [(volume_number, vocab.VolumeNumber(ident=''))],
# 			}
# 		
# 		if not issue_title:
# 			if issue_number:
# 				issue_title = f'Issue {issue_number} of “{title}”'
# 			else:
# 				issue_title = f'Issue of “{title}”'
# 
# 		identifiers = []
# 		if issue_number:
# 			identifiers.append(vocab.IssueNumber(ident='', content=issue_number))
# 
# 		issue_uri = helper.make_proj_uri('Journal', aata_id, 'Issue', issue_id)
# 		issues.append({
# 			'uri': issue_uri,
# 			'object_type': vocab.IssueText,
# 			'label': issue_title,
# 			'_aata_record_id': issue_id,
# 			'translations': list(issue_translations),
# 			'identifiers': identifiers,
# 			'volume': volume_number,
# 			'referred_to_by': notes,
# 		})
# 
# 	make_la_lo = MakeLinkedArtLinguisticObject()
# 	for volume in volumes.values():
# 		make_la_lo(volume)
# 
# 	previous = None
# 	# TODO: where did this data go?
# # 	for jh in e.xpath('./journal_group/journal_history'):
# # 		history_id = jh.findtext('./linked_journal_id')
# # 		history_name = jh.findtext('./linked_journal_name')
# # 		history_reason = jh.findtext('./change_reason')
# # 		huri = helper.make_proj_uri('Journal', history_id)
# # 		previous = {
# # 			'uri': huri,
# # 			'uri_components': ('Journal', history_id),
# # 			'label': history_name,
# # 			'_aata_record_id': history_id,
# # 			'reason': history_reason
# # 		}
# 
# 	var_titles = [(var_title, variantTitleIdentifier(ident='')) for var_title in var_title_strings]
# 
# 	data = {
# 		# TODO: lang_name
# 		# TODO: lang_scope
# 		'parent': record,
# 		'uri': journal_uri,
# 		'uri_components': ('Journal', aata_id),
# 		'label': title,
# 		'_aata_record_id': aata_id,
# 		'translations': list(translations),
# 		'identifiers': issn + var_titles,
# 		'issues': issues,
# 		'years': [start_year, cease_year],
# 		'volumes': volumes,
# 		'publishers': publishers,
# 		'sponsors': sponsors,
# 		'previous': previous,
# 		'object_type': vocab.JournalText,
# 	}
# 	
# 	return {k: v for k, v in data.items() if v is not None}
# 
# def _extract_series(record, helper):
# 	'''Extract information about a series record XML element'''
# 	rdg = record['record_desc_group']
# 	sg = record['series_group']
# 	
# 	aata_id = rdg['record_id']
# 	title = sg['title']
# 	var_title = sg.get('variant_title')
# 	translations = [t for t in _as_list(sg.get('title_translated'))]
# 
# # 	lang_name = e.findtext('./series_group/language/lang_name')
# # 	lang_scope = e.findtext('./series_group/language/lang_scope')
# 
# 	start_year = sg.get('start_year')
# 	cease_year = sg.get('cease_year')
# 	
# 	series_uri = helper.make_proj_uri('Series', aata_id)
# 
# 	issn_strs = sg.get('issn', [])
# 	if not isinstance(issn_strs, list):
# 		issn_strs = [issn_strs]
# 	
# 	issn = [(issn, vocab.IssnIdentifier(ident='')) for issn in issn_strs]
# 
# 	publishers = [_extract_publisher_group(pg, helper) for pg in _as_list(record.get('publisher_group'))]
# 	sponsors = [_extract_sponsor_group(sg, helper) for sg in _as_list(record.get('sponsor_group'))]
# 
# 	previous = None
# 	# TODO: where did this data go?
# # 	for jh in e.xpath('./series_group/series_history'):
# # 		history_id = jh.findtext('./linked_series_id')
# # 		history_name = jh.findtext('./linked_series_name')
# # 		history_reason = jh.findtext('./change_reason')
# # 		huri = helper.make_proj_uri('Series', history_id)
# # 		previous = {
# # 			'uri': huri,
# # 			'object_type': vocab.SeriesText,
# # 			'uri_components': ('Series', history_id),
# # 			'label': history_name,
# # 			'_aata_record_id': history_id,
# # 			'reason': history_reason
# # 		}
# 
# 	var_titles = [(var_title, variantTitleIdentifier(ident=''))] if var_title is not None else []
# 
# 	data = {
# 		# TODO: lang_name
# 		# TODO: lang_scope
# 		'uri': series_uri,
# 		'parent': record,
# 		'uri_components': ('Series', aata_id),
# 		'label': title,
# 		'_aata_record_id': aata_id,
# 		'translations': list(translations),
# 		'identifiers': issn + var_titles,
# 		'years': [start_year, cease_year],
# 		'publishers': publishers,
# 		'sponsors': sponsors,
# 		'previous': previous,
# 	}
# 	
# 	return {k: v for k, v in data.items() if v is not None}
# 
# 
# def _extract_article(data, language_code_map, helper):
# 	'''Extract information about an "article" record XML element'''
# 	rdg = data['record_desc_group']
# 	doc_code = rdg['doc_type']['doc_code']
# 
# 	primary = data['title_group']['primary']
# 	title = primary.get('title')
# 	var_title = primary.get('title_variant')
# 	tt = primary.get('title_translated')
# 	translations = _as_list(tt)
# 
# 	ng = data['notes_group']
# 	ld = ng.get('lang_doc')
# 	doc_langs = set(_as_list(ld))
# 	ls = ng.get('lang_summary')
# 	sum_langs = set(_as_list(ls))
# 
# 	isbn_refs = ng.get('isbn', [])
# 	if not isinstance(isbn_refs, list):
# 		isbn_refs = [isbn_refs]
# 
# 	issn_strs = ng.get('issn', [])
# 	if not isinstance(issn_strs, list):
# 		issn_strs = [issn_strs]
# 	
# 	issn = [(issn, vocab.IssnIdentifier(ident='')) for issn in issn_strs]
# 
# 	isbn = []
# 	qualified_identifiers = []
# 	for isbn_ref in isbn_refs:
# 		n = isbn_ref['isbn_number']
# 		pair = (n, vocab.IsbnIdentifier())
# 		q = isbn_ref.get('isbn_qualifier')
# 		if q is None or not q:
# 			isbn.append(pair)
# 		else:
# 			notes = (vocab.Note(ident='', content=q),)
# # 			print(f'ISBN: {n} [{q}]')
# 			qualified_identifiers.append((n, vocab.IsbnIdentifier, notes))
# 
# 	
# 	rig = data['record_id_group']
# 	aata_id = rig['record_id']
# 	localIds = [vocab.LocalNumber(content=aata_id)]
# 
# 	classifications = []
# 	code_type = None # TODO: is there a model.Type value for this sort of code?
# 	cgroups = _as_list(data['classification_group'])
# 	for cg in cgroups:
# 		# TODO: there are only 61 unique classifications in AATA data; map these to UIDs
# 		cid = cg['class_code']
# 		label = cg['class_name']
# 
# 		name = vocab.PrimaryName(content=label)
# 		classification = model.Type(label=label)
# 		classification.identified_by = name
# 
# 		code = model.Identifier(content=cid)
# 
# 		code.classified_as = code_type
# 		classification.identified_by = code
# 		classifications.append(classification)
# 
# 	part_of = []
# 	make_la_lo = MakeLinkedArtLinguisticObject()
# 	if 'collective_rec_id' in rig:
# 		# this is the upward pointing relation between, e.g., chapters and books
# 		cid = rig['collective_rec_id']
# 		collective = make_la_lo({
# 			'uri': helper.make_proj_uri('LinguisticObject', cid)
# 		})
# 		part_of.append(collective)
# 
# 	indexings = []
# 	for ig in _as_list(data.get('index_group')):
# 		try:
# 			aid = ig['gaia_auth_id']
# 			atype = ig['gaia_auth_type']
# 			label = ig['index_term']
# 			itype = _gaia_authority_type(atype)
# 		except:
# 			pprint.pprint(ig)
# 			raise
# 		name = vocab.Title()
# 		name.content = label
# 
# 		index = itype(label=label)
# 		index.identified_by = name
# 
# 		code = model.Identifier(content=aid)
# 
# 		code.classified_as = code_type
# 		index.identified_by = code
# 		indexings.append(index)
# 
# 	if title is not None and len(doc_langs) == 1:
# 		code = doc_langs.pop()
# 		try:
# 			language = language_object_from_code(code, language_code_map)
# 			if language is not None:
# 				title = (title, language)
# 		except:
# 			pass
# 
# 	var_titles = [(var_title, variantTitleIdentifier(ident=''))] if var_title is not None else []
# 
# 	id_components = ['LinguisticObject', aata_id]
# 	return {
# 		'parent': data,
# 		'label': title,
# 		'document_languages': doc_langs,
# 		'summary_languages': sum_langs,
# 		'_document_type': doc_code,
# 		'_aata_record_id': aata_id,
# 		'translations': list(translations),
# 		'identifiers': localIds + isbn + issn + var_titles,
# 		'qualified_identifiers': qualified_identifiers,
# 		'classifications': classifications,
# 		'indexing': indexings,
# 		'part_of': part_of,
# 		'uri': helper.make_proj_uri(*id_components),
# 	}
# 
# def _extract_abstracts(data, aata_id):
# 	'''Extract information about abstracts from an "article" record XML element'''
# 	record = data['parent']
# 	rids = [record['record_id_group']['record_id']]
# 	lids = [i for i in record['record_id_group']['aata_id']]
# 	ags = [record['abstract_group']]
# 	
# 	abstracts = []
# 	for i, ag in enumerate(ags):
# 		content = ag.get('abstract')
# 		author_abstract_flag = ag['author_abstract_flag']
# 		if ag is not None:
# 			language = ag.get('@lang')
# 
# 			localIds = [vocab.LocalNumber(ident='', content=i) for i in rids]
# 			legacyIds = [(i, legacyIdentifier) for i in lids]
# 			abstract = {
# 				'_aata_record_id': aata_id,
# 				'_aata_record_abstract_seq': i,
# 				'content': content,
# 				'language': language,
# 				'author_abstract_flag': (author_abstract_flag == 'yes'),
# 				'identifiers': localIds + legacyIds,
# 			}
# 			abstracts.append(abstract)
# 	data['_abstracts'] = abstracts
# 
# 
# def _extract_organizations(data, aata_id, helper):
# 	'''Extract information about organizations from an "article" record XML element'''
# 	i = -1
# 	organizations = []
# 	record = data['parent']
# 	ig = record['imprint_group']
# 	
# 	keys = set(ig.keys())
# 	keys -= {'distributor', 'publisher'}
# 
# 	orgs = []
# 	for d in _as_list(ig.get('distributor', [])):
# 		oid = d['gaia_corp_id']
# 		name = d['distributor_name']
# 		loc = d['distributor_location']
# 		orgs.append({'role': 'distributor', 'gaia_corp_id': oid, 'name': name, 'loc': loc})
# 	for d in _as_list(ig.get('publisher', [])):
# 		oid = d['gaia_corp_id']
# 		name = d['publisher_name']
# 		loc = d['publisher_location']
# 		orgs.append({'role': 'publisher', 'gaia_corp_id': oid, 'name': name, 'loc': loc})
# 
# 	for i, org in enumerate(orgs):
# 		role = org['role']
# 		name = org['name']
# 		auth_id = org['gaia_corp_id']
# 		auth_type = 'CB' # TODO: used to come from the data in the 'gaia_auth_type' field
# 		org = {
# 			'_aata_record_id': aata_id,
# 			'_aata_record_organization_seq': i,
# 			'label': name,
# 			'role': role,
# # 			'properties': properties,
# 			'names': [(name,)],
# 			'object_type': _gaia_authority_type(auth_type),
# 			'identifiers': [vocab.LocalNumber(ident='', content=auth_id)],
# 			'uri': helper.make_proj_uri('Organization', auth_type, auth_id, name),
# 		}
# 		organizations.append(org)
# 	data['_organizations'] = organizations
# 
# def _extract_authors(data, aata_id, helper):
# 	'''Extract information about authors from an "article" record XML element'''
# 	record = data['parent']
# 	authorship_group = record.get('authorship_group', {}) or {}
# 	
# 	primary_authors = authorship_group.get('primary_author', [])
# 	parent_book_author = authorship_group.get('parent_book_author', [])
# 	if not isinstance(primary_authors, list):
# 		primary_authors = [primary_authors]
# 	if not isinstance(parent_book_author, list):
# 		parent_book_author = [parent_book_author]
# 
# 	authors = []
# 	for role_key, value in authorship_group.items():
# 		records = _as_list(value)
# 		for i, a in enumerate(records):
# 			for role in _as_list(a['author_role']):
# 				auth_id = a.get('gaia_authority_id')
# 				if auth_id is not None:
# 					name = a['author_name']
# 					auth_type = a['gaia_authority_type']
# 
# 					id_components = tuple()
# 					if auth_id is None:
# 						print('*** no gaia auth id for author in record %r' % (aata_id,))
# 						id_components = ('AATA' 'P', 'Internal', aata_id, i)
# 					else:
# 						id_components = ('AATA' 'P', auth_type, auth_id, name)
# 
# 					author = {
# 						'_aata_record_id': aata_id,
# 						'_aata_record_author_seq': i,
# 						'label': name,
# 						'names': [(name,)],
# 						'object_type': _gaia_authority_type(auth_type),
# 						'identifiers': [vocab.LocalNumber(ident='', content=auth_id)],
# 						'uri': helper.make_proj_uri(*id_components),
# 					}
# 
# 					if role is not None:
# 						author['creation_role'] = role
# 					else:
# 						print('*** No author role found for authorship group')
# 						pprint.pprint(a)
# 
# 					authors.append(author)
# 				else:
# 					print('*** No author_id found for record %s' % (aata_id,), file=sys.stderr)
# 					pprint.pprint(a)
# 	data['_authors'] = authors
# 
# @use('document_types')
# def add_aata_object_type(data, document_types):
# 	'''
# 	Given an "article" `dict` containing a `_document_type` key which has a two-letter
# 	document type string (e.g. 'JA' for journal article, 'BC' for book), add a new key
# 	`object_type` containing the corresponding `vocab` class. This class can be used to
# 	construct a model object for this "article".
# 
# 	For example, `add_aata_object_type({'_document_type': 'AV', ...})` returns the `dict`:
# 	`{'_document_type': 'AV', 'document_type': vocab.AudioVisualContent, ...}`.
# 	'''
# 	atype = data['_document_type']
# 	clsname = document_types[atype]
# 	data['object_type'] = getattr(vocab, clsname)
# 	return data

# imprint organizations chain (publishers, distributors)

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

# @use('language_code_map')
# def detect_title_language(data: dict, language_code_map):
# 	'''
# 	Given a `dict` representing a Linguistic Object, attempt to detect the language of
# 	the value for the `label` key.  If the detected langauge is also one of the languages
# 	asserted for the record's summaries or underlying document, then update the `label`
# 	to be a tuple consisting of the original label and a Language model object.
# 	'''
# 	dlangs = data.get('document_languages', set())
# 	slangs = data.get('summary_languages', set())
# 	languages = dlangs | slangs
# 	title = data.get('label')
# 	if isinstance(title, tuple):
# 		title = title[0]
# 	try:
# 		if title is None:
# 			return NOT_MODIFIED
# 		translations = data.get('translations', [])
# 		if translations and languages:
# 			detected = detect(title)
# 			threealpha = iso639.to_iso639_2(detected)
# 			if threealpha in languages:
# 				language = self.helper.language_object_from_code(threealpha)
# 				if language is not None:
# 					# we have confidence that we've matched the language of the title
# 					# because it is one of the declared languages for the record
# 					# document/summary
# 					data['label'] = (title, language)
# 					return data
# 			else:
# 				# the detected language of the title was not declared in the record data,
# 				# so we lack confidence to proceed
# 				pass
# 	except iso639.NonExistentLanguageError as e:
# 		print('*** Unrecognized language code detected: %r' % (detected,), file=sys.stderr)
# 	except KeyError as e:
# 		print(
# 			'*** LANGUAGE: detected but unrecognized title language %r '
# 			'(cf. declared in metadata: %r): %s' % (e.args[0], languages, title),
# 			file=sys.stderr
# 		)
# 	except Exception as e:
# 		print('*** detect_title_language error: %r' % (e,))
# 	return NOT_MODIFIED

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
	def __init__(self, input_path, abstracts_pattern, journals_pattern, series_pattern, people_pattern, **kwargs):
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
		self.people_pattern = people_pattern
		self.journals_pattern = journals_pattern
		self.series_pattern = series_pattern
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
			# write ARTICLES data
			_ = self.add_person_or_group_chain(graph, people, serialize=True)
		return people
		
	def add_articles_chain(self, graph, records, serialize=True):
		'''Add transformation of article records to the bonobo pipeline.'''
		articles = graph.add_chain(
			ExtractKeyedValue(key='record'),
			ModelArticle(helper=self.helper),
# 			add_aata_object_type,
# 			detect_title_language,
# 			MakeLinkedArtLinguisticObject(),
# 			AddArchesModel(model=self.models['LinguisticObject']),
# 			add_imprint_orgs,
			_input=records.output
		)

		people = graph.add_chain(ExtractKeyedValues(key='_people'), _input=articles.output)
		events = graph.add_chain(ExtractKeyedValues(key='_events'), _input=articles.output)

		if serialize:
			# write ARTICLES data
			self.add_serialization_chain(graph, articles.output, model=self.models['LinguisticObject'])
			self.add_serialization_chain(graph, events.output, model=self.models['Event'])
			_ = self.add_person_or_group_chain(graph, people, serialize=True)
		return articles

# 	def add_articles_chain_old(self, graph, records, serialize=True):
# 		'''Add transformation of article records to the bonobo pipeline.'''
# 		articles = graph.add_chain(
# 			MakeAATAArticleDict(helper=self.helper),
# 			add_aata_object_type,
# 			detect_title_language,
# 			MakeLinkedArtLinguisticObject(),
# 			AddArchesModel(model=self.models['LinguisticObject']),
# 			add_imprint_orgs,
# 			_input=records.output
# 		)
# 		if serialize:
# 			# write ARTICLES data
# 			self.add_serialization_chain(graph, articles.output, model=self.models['LinguisticObject'])
# 		return articles
# 
# 	def add_people_chain(self, graph, articles, serialize=True):
# 		'''Add transformation of author records to the bonobo pipeline.'''
# 		def trace(d):
# 			print(get_crom_object(d))
# 			return d
# 		
# 		articles_with_authors = graph.add_chain(
# 			add_aata_authors,
# 			_input=articles.output
# 		)
# 
# 		if serialize:
# 			# write ARTICLES with their authorship/creation events data
# 			self.add_serialization_chain(graph, articles_with_authors.output, model=self.models['LinguisticObject'])
# 
# 		people = graph.add_chain(
# 			ExtractKeyedValues(key='_authors'),
# 			_input=articles_with_authors.output
# 		)
# 		if serialize:
# 			# write PEOPLE data
# 			self.add_serialization_chain(graph, people.output, model=self.models['Person'])
# 		return people

	def add_abstracts_chain(self, graph, articles, serialize=True):
		'''Add transformation of abstract records to the bonobo pipeline.'''
		model_id = self.models.get('LinguisticObject', 'XXX-LinguisticObject-Model')
		abstracts = graph.add_chain(
			MakeAATAAbstract(helper=self.helper),
			AddArchesModel(model=self.models['LinguisticObject']),
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
			self.add_serialization_chain(graph, abstracts.output, model=self.models['LinguisticObject'])
		return abstracts

# 	def add_organizations_chain(self, graph, articles, key='organizations', serialize=True):
# 		'''Add transformation of organization records to the bonobo pipeline.'''
# 		organizations = graph.add_chain(
# 			ExtractKeyedValues(key=key),
# 			MakeLinkedArtOrganization(),
# 			_input=articles.output
# 		)
# 		if serialize:
# 			# write ORGANIZATIONS data
# 			self.add_serialization_chain(graph, organizations.output, model=self.models['Group'])
# 		return organizations

	def _add_abstracts_graph(self, graph):
		abstract_records = graph.add_chain(
			MatchingFiles(path='/', pattern=self.abstracts_pattern, fs='fs.data.aata'),
			CurriedXMLReader(xpath='/AATA_XML/record', fs='fs.data.aata', limit=self.limit),
			_xml_element_to_dict,
		)
		articles = self.add_articles_chain(graph, abstract_records)
# 		self.add_people_chain(graph, articles)
# 		self.add_abstracts_chain(graph, articles)
# 		self.add_organizations_chain(graph, articles, key='organizations')
		return articles

	def _add_people_graph(self, graph):
		records = graph.add_chain(
			MatchingFiles(path='/', pattern=self.people_pattern, fs='fs.data.aata'),
			CurriedXMLReader(xpath='/auth_person_XML/record', fs='fs.data.aata', limit=self.limit),
			_xml_element_to_dict,
		)
		people = self.add_people_chain(graph, records)
		return people

# 	def _add_abstracts_graph_old(self, graph):
# 		abstract_records = graph.add_chain(
# 			MatchingFiles(path='/', pattern=self.abstracts_pattern, fs='fs.data.aata'),
# 			CurriedXMLReader(xpath='/AATA_XML/record', fs='fs.data.aata', limit=self.limit)
# 		)
# 		articles = self.add_articles_chain(graph, abstract_records)
# 		self.add_people_chain(graph, articles)
# 		self.add_abstracts_chain(graph, articles)
# 		self.add_organizations_chain(graph, articles, key='organizations')
# 		return articles
# 
# 	def _add_issues_chain(self, graph, journals, key='issues', serialize=True):
# 		issues = graph.add_chain(
# 			ExtractKeyedValues(key='issues'),
# 			make_issue,
# 			MakeLinkedArtLinguisticObject(),
# 			_input=journals.output
# 		)
# 		if serialize:
# 			# write ISSUES data
# 			self.add_serialization_chain(graph, issues.output, model=self.models['LinguisticObject'])
# 		return issues
# 
# 	def _add_journals_graph_old(self, graph, serialize=True):
# 		journals = graph.add_chain(
# 			MatchingFiles(path='/', pattern=self.journals_pattern, fs='fs.data.aata'),
# 			CurriedXMLReader(xpath='/journal_XML/record', fs='fs.data.aata', limit=self.limit),
# 			MakeAATAJournalDict(helper=self.helper),
# 			MakeLinkedArtLinguisticObject(),
# 			MakePublishingActivity(helper=self.helper),
# 			# Trace(name='journal', ordinals=list(range(100)))
# 		)
# 		
# 		publishers = self.add_organizations_chain(graph, journals, key='publishers', serialize=serialize)
# 		sponsors = self.add_organizations_chain(graph, journals, key='sponsors', serialize=serialize)
# 		issues = self._add_issues_chain(graph, journals, serialize=serialize)
# 		
# 		if serialize:
# 			# write ARTICLES data
# 			self.add_serialization_chain(graph, journals.output, model=self.models['Journal'])
# 		return journals
# 
# 	def _add_series_graph_old(self, graph, serialize=True):
# 		series = graph.add_chain(
# 			MatchingFiles(path='/', pattern=self.series_pattern, fs='fs.data.aata'),
# 			CurriedXMLReader(xpath='/series_XML/record', fs='fs.data.aata', limit=self.limit),
# 			MakeAATASeriesDict(helper=self.helper),
# 			MakeLinkedArtLinguisticObject(),
# 			MakePublishingActivity(helper=self.helper),
# # 			Trace(name='series')
# 		)
# 
# 		publishers = self.add_organizations_chain(graph, series, key='publishers', serialize=serialize)
# 		sponsors = self.add_organizations_chain(graph, series, key='sponsors', serialize=serialize)
# 
# 		if serialize:
# 			# write ARTICLES data
# 			self.add_serialization_chain(graph, series.output, model=self.models['Series'])
# 		return series

	def _construct_graph(self):
		graph = bonobo.Graph()

		articles = self._add_abstracts_graph(graph)
		people = self._add_people_graph(graph)

# 		articles = self._add_abstracts_graph_old(graph)
# 		journals = self._add_journals_graph_old(graph)
# 		series = self._add_series_graph_old(graph)

		self.graph = graph
		return graph

	def get_graph(self, **kwargs):
		'''Construct the bonobo pipeline to fully transform AATA data from XML to JSON-LD.'''
		if not self.graph:
			self._construct_graph()
		return self.graph

	def run(self, **options):
		'''Run the AATA bonobo pipeline.'''
		print("- Limiting to %d records per file" % (self.limit,), file=sys.stderr)
		services = self.get_services(**options)
		graph = self.get_graph(**options, services=services)
		self.run_graph(graph, services=services)

		print('Serializing static instances...', file=sys.stderr)
		for model, instances in self.static_instances.used_instances().items():
			g = bonobo.Graph()
			nodes = self.serializer_nodes_for_model(model=self.models[model])
			values = instances.values()
			source = g.add_chain(GraphListSource(values))
			self.add_serialization_chain(g, source.output, model=self.models[model])
			self.run_graph(g, services={})

class AATAFilePipeline(AATAPipeline):
	'''
	AATA pipeline with serialization to files based on Arches model and resource UUID.

	If in `debug` mode, JSON serialization will use pretty-printing. Otherwise,
	serialization will be compact.
	'''
	def __init__(self, input_path, abstracts_pattern, journals_pattern, series_pattern, people_pattern, **kwargs):
		super().__init__(input_path, abstracts_pattern, journals_pattern, series_pattern, people_pattern, **kwargs)
		self.use_single_serializer = False
		self.output_path = kwargs.get('output_path')

	def serializer_nodes_for_model(self, model=None):
		nodes = []
		if self.debug:
			nodes.append(MergingFileWriter(directory=self.output_path, partition_directories=True, compact=False, model=model))
		else:
			nodes.append(MergingFileWriter(directory=self.output_path, partition_directories=True, compact=True, model=model))
		return nodes
