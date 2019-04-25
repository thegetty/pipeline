# TODO: move cromulent model code out of this module

# AATA Extracters

import pprint
from bonobo.config import use
import copy
import uuid
import hashlib
import lxml.etree

from cromulent import model, vocab
from extracters.cleaners import date_cleaner, share_parse
from .basic import fetch_uuid, get_actor_type, get_aat_label

# utility functions

def language_object_from_code(code):
	languages = {
		'eng': {'ident': 'http://vocab.getty.edu/aat/300388277', 'label': 'English'}
	}
	try:
		kwargs = languages[code]
		return model.Language(**kwargs)
	except KeyError:
		print('*** No AAT link for language %r' % (language,))

# main article chain

def make_aata_article_dict(e):
	doc_type = e.findtext('./record_desc_group/doc_type')
	title_type = e.findtext('./title_group/title_type')
	title = e.findtext('./title_group[title_type = "Analytic"]/title')
	rid = e.findtext('./record_id_group/record_id')
	organizations = list(_xml_extract_organizations(e))
	authors = list(_xml_extract_authors(e))
	abstracts = list(_xml_extract_abstracts(e))
		
	return {
		'_source_element': e,
		'label': title,
		'_document_type': e.findtext('./record_desc_group/doc_type'),
		'_organizations': list(organizations),
		'_authors': list(authors),
		'_abstracts': list(abstracts),
		'uid': 'AATA-%s-%s-%s' % (doc_type, rid, title)
	}

def _xml_extract_abstracts(e):
	localIdentifier = None # TODO: aat:LocalIdentifier?
	legacyIdentifier = None # TODO: aat:LegacyIdentifier?

	rids = [e.text for e in e.findall('./record_id_group/record_id')]
	lids = [e.text for e in e.findall('./record_id_group/legacy_id')]
	for ag in e.xpath('./abstract_group'):
		a = ag.find('./abstract')
		author_abstract_flag = ag.findtext('./author_abstract_flag')
		if a is not None:
			content = a.text
			language = a.attrib.get('lang')
			
			localIds = [(i, localIdentifier) for i in rids]
			legacyIds = [(i, legacyIdentifier) for i in lids]
			yield {
				'content': content,
				'language': language,
				'author_abstract_flag': (author_abstract_flag == 'yes'),
				'identifiers': localIds + legacyIds,
			}

def _xml_extract_organizations(e):
	localIdentifier = None # TODO: aat:LocalIdentifier?
	legacyIdentifier = None # TODO: aat:LegacyIdentifier?

	for ig in e.xpath('./imprint_group/related_organization'):
		role = ig.findtext('organization_type')
		for o in ig.xpath('./organization'):
			aid = o.find('./organization_id')
			if aid is not None:
				name = aid.findtext('display_term')
				auth_id = aid.findtext('gaia_auth_id')
				auth_type = aid.findtext('gaia_auth_type')
				localIdentifier = None # TODO: aat:LocalIdentifier?
				yield {
					'label': name,
					'role': role,
					'names': [(name,)],
					'identifiers': [(auth_id, localIdentifier)],
					'uid': 'AATA-Org-%s-%s-%s' % (auth_type, auth_id, name)
				}
			else:
				print('*** No organization_id found for record %s:' % (o,))
				print(lxml.etree.tostring(ig).decode('utf-8'))

def _xml_extract_authors(e):
	localIdentifier = None # TODO: aat:LocalIdentifier?
	legacyIdentifier = None # TODO: aat:LegacyIdentifier?

	for ag in e.xpath('./authorship_group'):
		role = ag.findtext('author_role')
		for a in ag.xpath('./author'):
			aid = a.find('./author_id')
			if aid is not None:
				name = aid.findtext('display_term')
				auth_id = aid.findtext('gaia_auth_id')
				auth_type = aid.findtext('gaia_auth_type')
				localIdentifier = None # TODO: aat:LocalIdentifier?
				author = {}
				if role is not None:
					author['creation_role'] = role
				else:
					print('*** No author role found for authorship group in %s:' % (object,))
					print(lxml.etree.tostring(ag).decode('utf-8'))
				author.update({
					'label': name,
					'names': [(name,)],
					'identifiers': [(auth_id, localIdentifier)],
					'uid': 'AATA-P-%s-%s-%s' % (auth_type, auth_id, name)
				})
				yield author
			else:
				print('*** No author_id found for record %s:' % (parent,))
				print(lxml.etree.tostring(a).decode('utf-8'))

def add_aata_object_type(data):
	doc_types = { # should this be in settings (or elsewhere)?
		'AV': vocab.AudioVisualContent,
		'BA': vocab.Chapter,
		'BC': vocab.Monograph, # TODO: is this right?
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

def make_aata_imprint_orgs(data):
	e = data['_source_element']
	object = data['_LOD_OBJECT']
	organizations = data['_organizations']
	for o in organizations:
		event = model.Activity()
		object.used_for = event
		role = o.get('role')
		if role is not None:
			activity_names = {
				'Distributor': 'Distributing',
				'Publisher': 'Publishing',
				# TODO: Handle roles: Organization, Sponsor, University
			}
			if role in activity_names:
				event._label = activity_names[role]
			else:
				print('*** No/unknown organization role (%r) found for imprint_group in %s:' % (role, object,))
				print(lxml.etree.tostring(ig).decode('utf-8'))
		
		org = {k: v for k, v in o.items()}
		org.update({
			'events': [event],
			'parent': object,
			'parent_data': data,
		})
		yield org

# article authors chain

def make_aata_authors(data):
	object = data['_LOD_OBJECT']
	event = model.Creation()
	object.created_by = event
	
	for a in data.get('_authors', []):
		subevent = model.Creation()
		event.part = subevent
		role = a.get('creation_role')
		if role is not None:
			subevent._label = role
		author = {k: v for k, v in a.items()}
		author.update({
			'parent': object,
			'parent_data': data,
			'events': [subevent],
		})
		yield author

# article abstract chain

def make_aata_abstract(author_data):
	data = author_data['parent_data']
	object = data['_LOD_OBJECT']
	for a in data.get('_abstracts', []):
		abstract = model.LinguisticObject()
		if a.get('author_abstract_flag'):
			event = model.Creation()
			abstract.created_by = event
			event.carried_out_by = author_data['_LOD_OBJECT']
		else:
			# TODO: what about the creation event when author_abstract_flag != 'yes'?
			pass

		abstract_dict = {k: v for k, v in a.items()}

		abstract.content = a.get('content')
		abstract.classified_as = model.Type(ident='http://vocab.getty.edu/aat/300026032', label='Abstract') # TODO: is this the right aat URI?
		abstract.refers_to = object
		langcode = a.get('language')
		if langcode is not None:
			l = language_object_from_code(langcode)
			if l is not None:
				abstract.language = l
				abstract_dict['language'] = l

		abstract_dict = {k: v for k, v in a.items()}
		abstract_dict.update({
			'_LOD_OBJECT': abstract,
			'parent': object,
			'uid': 'AATA-A-%s-%s' % (data['uid'], hashlib.sha224(a['content'].encode('utf-8')).hexdigest())
		})
		yield abstract_dict
