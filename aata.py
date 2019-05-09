#!/usr/bin/env python3 -B

# TODO: ensure that multiple serializations to the same uuid are merged.
#       e.g. a journal article with two authors, that each get asserted
#       as carrying out the creation event.

import os
import sys
import bonobo

from pipeline.nodes.basic import Serializer
from pipeline.projects.aata import AATAPipeline
from pipeline.io.arches import ArchesWriter, FileWriter
from settings import aata_data_path, output_file_path, arches_models, DEBUG

### Pipeline

class AATAFilePipeline(AATAPipeline):
	'''
	AATA pipeline with serialization to files based on Arches model and resource UUID.

	If in `debug` mode, JSON serialization will use pretty-printing. Otherwise,
	serialization will be compact.
	'''
	def __init__(self, input_path, files_pattern, **kwargs):
		super().__init__(input_path, files_pattern, **kwargs)
		debug = kwargs.get('debug', False)
		output_path = kwargs.get('output_path')
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
