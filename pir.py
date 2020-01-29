#!/usr/bin/env python3 -B

import os
import sys
import csv
import bonobo
from cromulent import model, vocab
from cromulent.model import factory

from pipeline.projects.provenance import ProvenanceFilePipeline, ProvenancePipeline
from settings import project_data_path, output_file_path, arches_models, DEBUG

### Pipeline

if __name__ == '__main__':
	factory.cache_hierarchy()
	if DEBUG:
		LIMIT		= int(os.environ.get('GETTY_PIPELINE_LIMIT', 10))
	else:
		LIMIT		= int(os.environ.get('GETTY_PIPELINE_LIMIT', 10000000))

	catalogs = {
		'header_file': 'sales_catalogs_info_0.csv',
# 		'files_pattern': 'sales_catalogs_info.csv',
		'files_pattern': 'sales_catalogs_info_non_auctions.csv',
	}
	contents = {
		'header_file': 'sales_contents_0.csv',
# 		'files_pattern': 'sales_contents_[!0]*.csv',
		'files_pattern': 'sales_contents_non_auctions.csv',
	}
	auction_events = {
		'header_file': 'sales_descriptions_0.csv',
# 		'files_pattern': 'sales_descriptions.csv',
		'files_pattern': 'sales_descriptions_non_auctions.csv',
	}

#	factory.production_mode()
	vocab.add_linked_art_boundary_check()
	vocab.add_attribute_assignment_check()

	print_dot = False
	if 'dot' in sys.argv[1:]:
		print_dot = True
		sys.argv[1:] = [a for a in sys.argv[1:] if a != 'dot']
	parser = bonobo.get_argument_parser()
	with bonobo.parse_args(parser) as options:
		try:
			pir_data_path = project_data_path('provenance')
			pipeline = ProvenanceFilePipeline( # ProvenancePipeline
				pir_data_path,
				catalogs=catalogs,
				auction_events=auction_events,
				contents=contents,
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
