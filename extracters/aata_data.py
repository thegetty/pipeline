# TODO: move cromulent model code out of this module

# AATA Extracters

from cromulent import model, vocab
from bonobo.config import use
from extracters.cleaners import date_cleaner, share_parse
from .basic import fetch_uuid, get_actor_type, get_aat_label
import copy
import lxml.etree

def language_object_from_code(code):
	languages = {
		'eng': {'ident': 'http://vocab.getty.edu/aat/300388277', 'label': 'English'}
	}
	try:
		kwargs = languages[code]
		return model.Language(**kwargs)
	except KeyError:
		print('*** No AAT link for language %r' % (language,))

def make_aata_article_dict(e):
	doc_type = e.findtext('./record_desc_group/doc_type')
	title_type = e.findtext('./title_group/title_type')
	title = e.findtext('./title_group[title_type = "Analytic"]/title')
	rid = e.findtext('./record_id_group/record_id')
	return {
		'_source_element': e,
		'label': title,
		'uid': 'AATA-%s-%s-%s' % (doc_type, rid, title)
	}

def make_aata_author_dict(e, parent_object, parent_data, event):
	aid = e.find('./author_id')
	if aid is not None:
		name = aid.findtext('display_term')
		auth_id = aid.findtext('gaia_auth_id')
		auth_type = aid.findtext('gaia_auth_type')
		yield {
			'_source_element': e,
			'parent': parent_object,
			'parent_data': parent_data,
			'label': name,
			'events': [event],
			'uid': 'AATA-P-%s-%s-%s' % (auth_type, auth_id, name)
		}
	else:
		print('*** No author_id found for record %s:' % (parent,))
		print(lxml.etree.tostring(e).decode('utf-8'))

def make_aata_authors(data):
	e = data['_source_element']
	object = data['_LOD_OBJECT']
	event = model.Creation()
	object.created_by = event
	for ag in e.xpath('./authorship_group'):
		subevent = model.Creation()
		event.part = subevent
		role = ag.findtext('author_role')
		if role is None:
			print('*** No author role found for authorship group in %s:' % (object,))
			print(lxml.etree.tostring(ag).decode('utf-8'))
		subevent._label = role
		for a in ag.xpath('./author'):
			yield from make_aata_author_dict(a, parent_object=object, parent_data=data, event=subevent)

def make_aata_abstract(data):
	author_data = data
	data = author_data['parent_data']
	e = data['_source_element']
	object = data['_LOD_OBJECT']
	for ag in e.xpath('./abstract_group'):
		abstract = model.LinguisticObject()
		a = ag.find('./abstract')
		author_abstract_flag = ag.findtext('./author_abstract_flag')
		if a is not None:
			if author_abstract_flag == 'yes':
				event = model.Creation()
				abstract.created_by = event
				event.carried_out_by = author_data['_LOD_OBJECT']
			else:
				# TODO: what about the creation event when author_abstract_flag != 'yes'?
				pass

			content = a.text
			language = a.attrib.get('lang')
			abstract.content = content
			abstract.classified_as = model.Type(ident='http://vocab.getty.edu/aat/300026032', label='Abstract') # TODO: is this the right aat URI?
			if language is not None:
				l = language_object_from_code(language)
				if l is not None:
					abstract.language = l

			abstract.refers_to = object
			yield {
				'_LOD_OBJECT': abstract,
				'_source_element': ag,
				'parent': object,
				'content': content,
				'uid': 'AATA-A-%s-%s' % (data['uid'], content)
			}

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
	e = data['_source_element']
	atype = e.findtext('./record_desc_group/doc_type')
	data['object_type'] = doc_types[atype]
	return data

