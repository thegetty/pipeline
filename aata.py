#!/usr/bin/env python3 -B

# TODO: ensure that multiple serializations to the same uuid are merged.
#       e.g. a journal article with two authors, that each get asserted
#       as carrying out the creation event.

import os
import sys
import bonobo

from pipeline.projects.aata import AATAFilePipeline
from settings import project_data_path, output_file_path, arches_models, DEBUG

### Pipeline

if __name__ == '__main__':
	if DEBUG:
		LIMIT		= int(os.environ.get('GETTY_PIPELINE_LIMIT', 10))
	else:
		LIMIT		= int(os.environ.get('GETTY_PIPELINE_LIMIT', 10000000))
	xml_files_pattern = '*.xml'
	print_dot = False
	if 'dot' in sys.argv[1:]:
		print_dot = True
		sys.argv[1:] = [a for a in sys.argv[1:] if a != 'dot']
	parser = bonobo.get_argument_parser()
	with bonobo.parse_args(parser) as options:
		try:
			aata_data_path = project_data_path('aata')
			pipeline = AATAFilePipeline(
				aata_data_path,
				xml_files_pattern,
				output_path=output_file_path,
				models=arches_models,
				limit=LIMIT,
				debug=DEBUG
			)
			if print_dot:
				print(pipeline.get_graph()._repr_dot_())
			else:
				pipeline.run(**options)
		except RuntimeError:
			raise ValueError()
