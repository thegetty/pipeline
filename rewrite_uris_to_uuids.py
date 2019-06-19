#!/usr/bin/env python3 -B

import os
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

def filename_for(data: dict, original_filename: str):
	'''
	For JSON `data` read from the file `original_filename`, return the filename to which
	it should be (re-)written. The new filename is based on the top-level 'id' member,
	which should be a UUID URN.
	
	If no valid UUID is found, returns `original_filename`.
	'''
	if 'id' not in data:
		print(f'*** no @id found for {original_filename}')
		return original_filename
	uri = data['id']
	if not uri.startswith('urn:uuid:'):
		print(f'*** @id does not appear to be a UUID URN in {original_filename}')
		return original_filename
	urn = uri[len('urn:uuid:'):]
	fn = f'{urn}.json'
	p = Path(original_filename)
	q = p.with_name(fn)
	return q

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
files = list(p.rglob('*.json'))
for i, f in enumerate(files):
	print(f'{i} {f}')
	with open(f) as data_file:    
		data = json.load(data_file)
	d = r.rewrite(data)
	newfile = filename_for(d, original_filename=f)
	with open(newfile, 'w') as data_file:
		json.dump(d, data_file, indent=2)
	if newfile != f:
		os.remove(f)
