#!/usr/bin/env python3

# TODO: refactor code from knoedler files (e.g. knoedler_linkedart.*) that is actually just linkedart related
# TODO: ensure that multiple serializations to the same uuid are merged. e.g. a journal article with two authors, that each get asserted as carrying out the creation event.

import os
import bonobo

from extracters.basic import Serializer
from extracters.aata_data import AATAPipeline
from extracters.arches import ArchesWriter, FileWriter
from settings import aata_data_path, output_file_path, arches_models, DEBUG

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
	xml_files = [f for f in os.listdir(aata_data_path) if f.endswith('.xml')]
	parser = bonobo.get_argument_parser()
	with bonobo.parse_args(parser) as options:
		try:
			pipeline = AATAFilePipeline(
				aata_data_path,
				xml_files,
				output_file_path,
				models=arches_models,
				limit=LIMIT,
				debug=DEBUG
			)
			pipeline.run(**options)
		except RuntimeError:
			raise ValueError()
