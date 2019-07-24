#!/usr/bin/env python3 -B

import os
import sys
import bonobo

from pipeline.projects.knoedler import KnoedlerFilePipeline
import settings
from cromulent import vocab

### Pipeline

if __name__ == '__main__':
	if settings.DEBUG:
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
