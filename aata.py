#!/usr/bin/env python3 -B

# TODO: ensure that multiple serializations to the same uuid are merged.
#       e.g. a journal article with two authors, that each get asserted
#       as carrying out the creation event.

import os
import sys
import bonobo

from pipeline.projects.aata import AATAFilePipeline
from settings import project_data_path, output_file_path, arches_models, DEBUG
from cromulent import vocab

### Pipeline

if __name__ == '__main__':
	if DEBUG:
		LIMIT		= int(os.environ.get('GETTY_PIPELINE_LIMIT', 10))
	else:
		LIMIT		= int(os.environ.get('GETTY_PIPELINE_LIMIT', 10000000))
	abstracts_pattern = 'AATA_[0-9]*.xml'
	journals_pattern = 'AATA*Journal.xml'
	series_pattern = 'AATA*Series.xml'
	people_pattern = 'Auth_person.xml'

	vocab.add_linked_art_boundary_check()

	print_dot = False
	if 'dot' in sys.argv[1:]:
		print_dot = True
		sys.argv[1:] = [a for a in sys.argv[1:] if a != 'dot']
	parser = bonobo.get_argument_parser()
	with bonobo.parse_args(parser) as options:
		try:
			aata_data_path = project_data_path('aata')
			pipeline = AATAFilePipeline(
				input_path=aata_data_path,
				abstracts_pattern=abstracts_pattern,
				journals_pattern=journals_pattern,
				series_pattern=series_pattern,
				people_pattern=people_pattern,
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
