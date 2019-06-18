#!/usr/bin/env python3 -B

import sys
import json
import uuid
import pprint
import itertools
from pathlib import Path

from settings import output_file_path

class Rewriter:
	def __init__(self, prefix):
		self.map = {}
		self.prefix = prefix
		self.next = itertools.count(1000)

	def rewrite(self, d):
		if isinstance(d, dict):
			return {k: self.rewrite(v) for k, v in d.items()}
		elif isinstance(d, list):
			return [self.rewrite(v) for v in d]
		elif isinstance(d, int):
			return d
		elif isinstance(d, str):
			if d.startswith(self.prefix):
				d = d[len(self.prefix):]
				if d in self.map:
					return self.map[d]
				else:
					uu = str(uuid.uuid4())
					u = f'urn:uuid:{uu}'
					self.map[d] = u
					return u
			return d
		else:
			print(f'failed to rewrite JSON value: {d!r}')
			raise Exception()

if len(sys.argv) < 2:
	cmd = sys.argv[0]
	print(f'''
Usage: {cmd} URI_PREFIX

Process all json files in the output path (configured with the GETTY_PIPELINE_OUTPUT
environment variable), rewriting URIs that have the specified URI_PREFIX to urn:uuid:
URIs.
	'''.lstrip())
	sys.exit(1)

prefix = sys.argv[1]
r = Rewriter(prefix)
print(f'Rewriting JSON URIs with prefix {prefix!r}')
p = Path(output_file_path)
for i, f in enumerate(p.rglob('*.json')):
	print(f'{i} {f}')
	with open(f) as data_file:    
		data = json.load(data_file)
	d = r.rewrite(data)
	with open(f, 'w') as data_file:
		json.dump(d, data_file, indent=2)
