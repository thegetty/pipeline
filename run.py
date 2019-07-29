#!/usr/bin/env python3 -B

import os
import sys
import bonobo
import importlib
from cromulent import vocab

import settings

if len(sys.argv) < 2:
	print("python3 ./run.py project [dot] [*args]")
	sys.exit()
else:
	project = sys.argv[1]
	pipe = importlib.import_module(f'pipeline.projects.{project}')
	Pipeline = pipe.Pipeline
	sys.argv = [sys.argv[0], *sys.argv[2:]]

### Run the Pipeline

if __name__ == '__main__':
	if settings.DEBUG:
		LIMIT		= int(os.environ.get('GETTY_PIPELINE_LIMIT', 1))
		PACK_SIZE = 1
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
			pipeline = Pipeline(
				output_path=settings.output_file_path,
				models=settings.arches_models,
				pack_size=PACK_SIZE,
				limit=LIMIT,
				debug=settings.DEBUG
			)
			if print_dot:
				print(pipeline.get_graph()._repr_dot_())
			else:
				pipeline.run(**options)
		except RuntimeError:
			raise ValueError()
