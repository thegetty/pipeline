#!/usr/bin/env python3 -B

import os
import sys
import csv
import bonobo

from pipeline.projects.provenance import ProvenanceFilePipeline, ProvenancePipeline
from settings import pir_data_path, output_file_path, arches_models, DEBUG

### Pipeline

if __name__ == '__main__':
	if DEBUG:
		LIMIT		= int(os.environ.get('GETTY_PIPELINE_LIMIT', 10))
	else:
		LIMIT		= int(os.environ.get('GETTY_PIPELINE_LIMIT', 10000000))
	content_files_pattern = 'sales_contents_[!0].csv'
	header_file = 'sales_contents_0.csv'
	print_dot = False
	if 'dot' in sys.argv[1:]:
		print_dot = True
		sys.argv[1:] = [a for a in sys.argv[1:] if a != 'dot']
	parser = bonobo.get_argument_parser()
	with bonobo.parse_args(parser) as options:
		try:
			pipeline = ProvenanceFilePipeline( # ProvenancePipeline
				pir_data_path,
				header_file,
				files_pattern=content_files_pattern,
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
