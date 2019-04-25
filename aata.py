#!/usr/bin/env python3

# TODO: ensure that multiple serializations to the same uuid are merged. e.g. a journal article with two authors, that each get asserted as carrying out the creation event.
# TODO: move xml-related code to just the first extract node. all other nodes should be based on a resulting dict to allow re-use with non-xml source data.

import pprint
import sys, os
from sqlalchemy import create_engine
import bonobo
from bonobo.nodes import Limit
from bonobo.config import Configurable, Option
import itertools
import bonobo_sqlalchemy
import lxml.etree
import sqlite3

from cromulent import model, vocab
from extracters.xml import XMLReader, ExtractXPath, FilterXPathEqual, print_xml_element_text
from extracters.basic import AddArchesModel, AddFieldNames, Serializer, deep_copy, Offset, add_uuid, Trace
from extracters.knoedler_data import *
from extracters.knoedler_linkedart import *
from extracters.arches import ArchesWriter, FileWriter
from settings import *

# Set up environment
def get_services(**kwargs):
	return {
		'trace_counter': itertools.count(),
        'gpi': create_engine(gpi_engine),
        'aat': create_engine(aat_engine),
 		'uuid_cache': create_engine(uuid_cache_engine),
		'fs.data.aata': bonobo.open_fs(aata_data_path)
	}

### Pipeline

if DEBUG:
	print("In DEBUGGING mode")
	LIMIT		= os.environ.get('GETTY_PIPELINE_LIMIT', 10)
	PACK_SIZE	= 10
	SRLZ		= Serializer(compact=False)
	WRITER		= FileWriter(directory=output_file_path)
	# WRITER	= ArchesWriter()
else:
	LIMIT		= 10000000
	PACK_SIZE	= 10000000
	SRLZ		= Serializer(compact=True)
	WRITER		= FileWriter(directory=output_file_path)
	# WRITER	= ArchesWriter()


def make_author_dict(e, parent_object, parent_data, event):
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

def make_article_dict(e):
	doc_type = e.findtext('./record_desc_group/doc_type')
	title_type = e.findtext('./title_group/title_type')
	title = e.findtext('./title_group[title_type = "Analytic"]/title')
	rid = e.findtext('./record_id_group/record_id')
	return {
		'_source_element': e,
		'label': title,
		'uid': 'AATA-%s-%s-%s' % (doc_type, rid, title)
	}

class AddDataDependentArchesModel(Configurable):
	models = Option()
	def __call__(self, data):
		data['_ARCHES_MODEL'] = self.models['LinguisticObject']
		return data

def add_object_type(data):
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

def make_authors(data):
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
			yield from make_author_dict(a, parent_object=object, parent_data=data, event=subevent)

def make_la_record(data: dict):
	otype = data['object_type']
	object = otype(ident="urn:uuid:%s" % data['uuid'])
	object._label = data['label']
	return add_crom_data(data=data, what=object)

def language_object_from_code(code):
	languages = {
		'eng': {'ident': 'http://vocab.getty.edu/aat/300388277', 'label': 'English'}
	}
	try:
		kwargs = languages[code]
		return model.Language(**kwargs)
	except KeyError:
		print('*** No AAT link for language %r' % (language,))

def make_abstract(data):
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

def make_la_abstract(data: dict):
	return add_crom_data(data=data, what=data['_LOD_OBJECT'])

def get_graph(files, **kwargs):
	graph = bonobo.Graph()

	for f in files:
		aata_records = XMLReader(f, xpath='/AATA_XML/record', fs='fs.data.aata')
		articles = graph.add_chain(
			aata_records,
			Limit(LIMIT),
			make_article_dict,
			add_uuid,
			add_object_type,
			make_la_record,
			AddDataDependentArchesModel(models=arches_models),
		)
		
		if False:
			# write ARTICLES data
			graph.add_chain(
				SRLZ,
				WRITER,
				_input=articles.output
			)
		
		people = graph.add_chain(
			make_authors,
			AddArchesModel(model=arches_models['Person']),
			add_uuid,
			add_person_aat_labels,
			make_la_person,
			_input=articles.output
		)
		
		if True:
			# write PEOPLE data
			graph.add_chain(
				SRLZ,
				WRITER,
				_input=people.output
			)

		abstracts = graph.add_chain(
			make_abstract,
			AddArchesModel(model=arches_models['LinguisticObject']),
			add_uuid,
			make_la_abstract,
			_input=people.output
		)

		if True:
			# write ABSTRACTS data
			graph.add_chain(
				SRLZ,
				WRITER,
				_input=abstracts.output
			)

# 		graph.add_chain(
# 			make_publishers,
# 			SRLZ,
# 			WRITER,
# 			_input=articles.output
# 		)

	return graph


if __name__ == '__main__':
	files = [f for f in os.listdir(aata_data_path) if f.endswith('.xml')]
	parser = bonobo.get_argument_parser()
	with bonobo.parse_args(parser) as options:
		try:
			bonobo.run(
				get_graph(files=files, **options),
				services=get_services(**options)
			)
		except RuntimeError:
			raise ValueError()

