#!/usr/bin/env python3 -B

# TODO: ensure that multiple serializations to the same uuid are merged.
#       e.g. a journal article with two authors, that each get asserted
#       as carrying out the creation event.

import os
import bonobo

from pipeline.projects.aata import AATAFilePipeline
from settings import aata_data_path, output_file_path, arches_models, DEBUG

### Pipeline

if __name__ == '__main__':
	if DEBUG:
		LIMIT		= int(os.environ.get('GETTY_PIPELINE_LIMIT', 10))
	else:
		LIMIT		= int(os.environ.get('GETTY_PIPELINE_LIMIT', 10000000))
	xml_files_pattern = '*.xml'
	parser = bonobo.get_argument_parser()
	with bonobo.parse_args(parser) as options:
		try:
			pipeline = AATAFilePipeline(
				aata_data_path,
				xml_files_pattern,
				output_path=output_file_path,
				models=arches_models,
				limit=LIMIT,
				debug=DEBUG
			)
			pipeline.run(**options)
		except RuntimeError:
			raise ValueError()
