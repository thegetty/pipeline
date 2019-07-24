#!/usr/bin/env python3 -B

import sys, os
from sqlalchemy import create_engine
import bonobo
import itertools
import bonobo_sqlalchemy

from pipeline.projects.knoedler import KnoedlerFilePipeline
from pipeline.nodes.basic import AddArchesModel, AddFieldNames, Serializer, deep_copy, Offset, add_uuid, Trace
from pipeline.projects.knoedler.data import *
from pipeline.projects.knoedler.linkedart import *
from pipeline.io.file import FileWriter
from pipeline.io.arches import ArchesWriter
from pipeline.linkedart import make_la_person
from settings import DEBUG, SPAM, arches_models, output_file_path
import settings

### Pipeline

if __name__ == '__main__':
	if DEBUG:
		LIMIT		= int(os.environ.get('GETTY_PIPELINE_LIMIT', 10))
		PACK_SIZE = 10
	else:
		LIMIT		= int(os.environ.get('GETTY_PIPELINE_LIMIT', 10000000))
	PACK_SIZE = 10000000

	vocab.add_linked_art_boundary_check()

	print_dot = False
	if 'dot' in sys.argv[1:]:
		print_dot = True
		sys.argv[1:] = [a for a in sys.argv[1:] if a != 'dot']
	parser = bonobo.get_argument_parser()
	with bonobo.parse_args(parser) as options:
		try:
			pipeline = KnoedlerFilePipeline(
				output_path=output_file_path,
				models=arches_models,
				pack_size=PACK_SIZE,
				limit=LIMIT,
				debug=DEBUG
			)
			if print_dot:
				print(pipeline.get_graph()._repr_dot_())
			else:
				pipeline.run(**options)
		except RuntimeError:
			raise ValueError()
