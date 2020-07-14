#!/usr/bin/env python3 -B

import os
import sys
import json
import uuid
import pprint
import itertools
from pathlib import Path
from contextlib import suppress

from settings import output_file_path
from pipeline.util.rewriting import rewrite_output_files, JSONValueRewriter

class JSONIDRemovalRewriter:
	def __init__(self):
		self.paths = {
			'HumanMadeObject': [
				('produced_by', 'id'),
				('produced_by', 'part', 'id'),
				('destroyed_by', 'id'),
			]
		}

	def remove_path(self, data, path):
		if len(path) == 1:
			prop = path[0]
			with suppress(KeyError):
				del data[prop]
		else:
			head, *tail = path
			if head in data:
				child = data[head]
				if isinstance(child, list):
					for element in child:
						self.remove_path(element, tail)
				else:
					self.remove_path(child, tail)

	def rewrite(self, d, *args, file=None, **kwargs):
		try:
			type = d['type']
		except KeyError:
			return d
		
		if type in self.paths:
			data = json.loads(json.dumps(d))
			paths = self.paths[type]
			for path in paths:
				self.remove_path(data, path)
			return data
		else:
			return d

if __name__ == '__main__':
	print(f'Removing meaningless `id` properties ...')
	r = JSONIDRemovalRewriter()
	rewrite_output_files(r, parallel=True)
	print('Done')
