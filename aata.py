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
			filter_abstract_authors, \
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

### Pipeline

class AddDataDependentArchesModel(Configurable):
	models = Option()
	def __call__(self, data):
		data['_ARCHES_MODEL'] = self.models['LinguisticObject']
		return data

class AATAPipeline:
	def __init__(self, input_path, files, limit=None, debug=False):
		self.files = files
		self.limit = limit
		self.debug = debug
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
	def get_services(self, **kwargs):
		return {
			'trace_counter': itertools.count(),
			'gpi': create_engine(gpi_engine),
			'aat': create_engine(aat_engine),
			'uuid_cache': create_engine(uuid_cache_engine),
			'fs.data.aata': bonobo.open_fs(self.input_path)
		}

	def add_serialization_chain(self, graph, input):
		if self.writer is not None:
			graph.add_chain(
				self.serializer,
				self.writer,
				_input=input
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
			AddDataDependentArchesModel(models=arches_models),
			_input=records.output
		)
		if serialize:
			# write ARTICLES data
			self.add_serialization_chain(graph, articles.output)
		return articles

	def add_people_chain(self, graph, articles, serialize=True):
		people = graph.add_chain(
			make_aata_authors,
			AddArchesModel(model=arches_models['Person']),
			add_uuid,
			make_la_person,
			_input=articles.output
		)
		if serialize:
			# write PEOPLE data
			self.add_serialization_chain(graph, people.output)
		return people

	def add_abstracts_chain(self, graph, articles, serialize=True):
		abstracts = graph.add_chain(
			make_aata_abstract,
			AddArchesModel(model=arches_models['LinguisticObject']),
			add_uuid,
			MakeLinkedArtAbstract(),
			_input=articles.output
		)
		
		# for each author of an abstract...
		author_abstracts = graph.add_chain(filter_abstract_authors, _input=abstracts.output)
		self.add_people_chain(graph, author_abstracts)
		
		if serialize:
			# write ABSTRACTS data
			self.add_serialization_chain(graph, abstracts.output)
		return abstracts

	def add_organizations_chain(self, graph, articles, serialize=True):
		organizations = graph.add_chain(
			extract_imprint_orgs,
			CleanDateToSpan(key='publication_date'),
			make_aata_imprint_orgs,
			AddArchesModel(model='XXX-Organization-Model'), # TODO: model for organizations?
			add_uuid,
			MakeLinkedArtOrganization(),
			_input=articles.output
		)
		if serialize:
			# write ORGANIZATIONS data
			self.add_serialization_chain(graph, organizations.output)
		return organizations

	def get_graph(self, **kwargs):
		graph = bonobo.Graph()
		files = self.files[:]
		if self.debug:
			sys.stderr.write("Processing %s\n" % (files[0],))

		for f in files:
			records = graph.add_chain(XMLReader(f, xpath='/AATA_XML/record', fs='fs.data.aata'))
			articles = self.add_articles_chain(graph, records)
			people = self.add_people_chain(graph, articles)
			abstracts = self.add_abstracts_chain(graph, articles)
			organizations = self.add_organizations_chain(graph, articles)

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


class AATAFilePipeline(AATAPipeline):
	def __init__(self, input_path, files, output_path=None, limit=None, debug=False):
		super().__init__(input_path, files, limit=limit, debug=debug)
		if debug:
			self.serializer	= Serializer(compact=False)
			self.writer		= FileWriter(directory=output_path)
			# self.writer	= ArchesWriter()
		else:
			self.serializer	= Serializer(compact=True)
			self.writer		= FileWriter(directory=output_path)
			# self.writer	= ArchesWriter()


if __name__ == '__main__':
	if DEBUG:
		LIMIT		= int(os.environ.get('GETTY_PIPELINE_LIMIT', 10))
	else:
		LIMIT		= 10000000
	files = [f for f in os.listdir(aata_data_path) if f.endswith('.xml')]
	parser = bonobo.get_argument_parser()
	with bonobo.parse_args(parser) as options:
		try:
			pipeline = AATAFilePipeline(aata_data_path, files, output_file_path, limit=LIMIT, debug=DEBUG)
			pipeline.run(**options)
		except RuntimeError:
			raise ValueError()

