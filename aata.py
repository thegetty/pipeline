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
from extracters.aata_data import AATAPipeline
# from extracters.aata_data import \
# 			add_aata_object_type, \
# 			filter_abstract_authors, \
# 			make_aata_abstract, \
# 			make_aata_article_dict, \
# 			make_aata_authors, \
# 			make_aata_imprint_orgs, \
# 			extract_imprint_orgs, \
# 			CleanDateToSpan
from extracters.knoedler_linkedart import *
from extracters.arches import ArchesWriter, FileWriter
from extracters.linkedart import \
			MakeLinkedArtAbstract, \
			MakeLinkedArtLinguisticObject, \
			MakeLinkedArtOrganization
from settings import *

### Pipeline

class AATAFilePipeline(AATAPipeline):
	'''
	AATA pipeline with serialization to files based on Arches model and resource UUID.
	
	If in `debug` mode, JSON serialization will use pretty-printing. Otherwise,
	serialization will be compact.
	'''
	def __init__(self, input_path, files, output_path=None, models=None, limit=None, debug=False):
		super().__init__(input_path, files, models=models, limit=limit, debug=debug)
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
			pipeline = AATAFilePipeline(
				aata_data_path,
				files,
				output_file_path,
				models=arches_models,
				limit=LIMIT,
				debug=DEBUG
			)
			pipeline.run(**options)
		except RuntimeError:
			raise ValueError()

