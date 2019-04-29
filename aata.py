#!/usr/bin/env python3

# TODO: refactor code from knoedler files (e.g. knoedler_linkedart.*) that is actually just linkedart related
# TODO: ensure that multiple serializations to the same uuid are merged. e.g. a journal article with two authors, that each get asserted as carrying out the creation event.

import pprint
import sys, os
from sqlalchemy import create_engine
import bonobo
from bonobo.nodes import Limit
from bonobo.config import Configurable, Option
import itertools
import bonobo_sqlalchemy
import sqlite3

from cromulent import model, vocab
from extracters.xml import XMLReader
from extracters.basic import \
			add_uuid, \
			AddArchesModel, \
			AddFieldNames, \
			deep_copy, \
			Offset, \
			Serializer, \
			Trace
from extracters.aata_data import \
			add_aata_object_type, \
			make_aata_abstract, \
			make_aata_article_dict, \
			make_aata_authors, \
			make_aata_imprint_orgs, \
			extract_imprint_orgs, \
			CleanDateToSpan
from extracters.knoedler_linkedart import *
from extracters.arches import ArchesWriter, FileWriter
from extracters.linkedart import \
			MakeLinkedArtAbstract, \
			MakeLinkedArtLinguisticObject, \
			MakeLinkedArtOrganization
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
	sys.stderr.write("In DEBUGGING mode\n")
	LIMIT		= int(os.environ.get('GETTY_PIPELINE_LIMIT', 10))
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


class AddDataDependentArchesModel(Configurable):
	models = Option()
	def __call__(self, data):
		data['_ARCHES_MODEL'] = self.models['LinguisticObject']
		return data

def add_serialization(graph, input):
	graph.add_chain(
		SRLZ,
		WRITER,
		_input=input
	)

def add_articles_chain(graph, records):
	articles = graph.add_chain(
		Limit(LIMIT),
		make_aata_article_dict,
		add_uuid,
		add_aata_object_type,
		MakeLinkedArtLinguisticObject(),
		AddDataDependentArchesModel(models=arches_models),
		_input=records.output
	)
	if True:
		# write ARTICLES data
		add_serialization(graph, articles.output)
	return articles

def add_people_chain(graph, articles):
	people = graph.add_chain(
		make_aata_authors,
		AddArchesModel(model=arches_models['Person']),
		add_uuid,
		make_la_person,
		_input=articles.output
	)
	if True:
		# write PEOPLE data
		add_serialization(graph, people.output)
	return people

def add_abstracts_chain(graph, people):
	abstracts = graph.add_chain(
		make_aata_abstract,
		AddArchesModel(model=arches_models['LinguisticObject']),
		add_uuid,
		MakeLinkedArtAbstract(),
		_input=people.output
	)
	if True:
		# write ABSTRACTS data
		add_serialization(graph, abstracts.output)
	return abstracts

def add_organizations_chain(graph, articles):
	organizations = graph.add_chain(
		extract_imprint_orgs,
		CleanDateToSpan(key='publication_date'),
		make_aata_imprint_orgs,
		AddArchesModel(model='XXX-Organization-Model'), # TODO: model for organizations?
		add_uuid,
		MakeLinkedArtOrganization(),
		_input=articles.output
	)
	if True:
		# write ORGANIZATIONS data
		add_serialization(graph, organizations.output)
	return organizations

def get_graph(files, **kwargs):
	graph = bonobo.Graph()
	if DEBUG:
		files = [files[0]]
		sys.stderr.write("Processing %s\n" % (files[0],))

	for f in files:
		records = graph.add_chain(XMLReader(f, xpath='/AATA_XML/record', fs='fs.data.aata'))
		articles = add_articles_chain(graph, records)
		people = add_people_chain(graph, articles)
		abstracts = add_abstracts_chain(graph, people)
		organizations = add_organizations_chain(graph, articles)

	return graph


if __name__ == '__main__':
	files = [f for f in os.listdir(aata_data_path) if f.endswith('.xml')]
	parser = bonobo.get_argument_parser()
	with bonobo.parse_args(parser) as options:
		try:
			graph = get_graph(files=files, **options)
			services = get_services(**options)
			bonobo.run(
				graph,
				services=services
			)
		except RuntimeError:
			raise ValueError()

