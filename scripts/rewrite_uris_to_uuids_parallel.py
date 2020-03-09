#!/usr/bin/env python3 -B

import os
import sys
import json
import time
import uuid
import pprint
import base64
import itertools
from pathlib import Path
from contextlib import suppress
import multiprocessing

from settings import output_file_path
from pipeline.util.rewriting import rewrite_output_files

class UUIDRewriter:
	def __init__(self, prefix, map_file=None):
		self.map = {}
		self.prefix = prefix
		self.map_file = map_file
		if map_file:
			# Load JSON map file for pre-written UUIDs
			with suppress(FileNotFoundError):
				with open(map_file) as fh:
					self.map = json.load(fh)

	def persist_map(self):
		with open(self.map_file, 'w') as fh:
			json.dump(self.map, fh)

	def rewrite(self, d, *args, **kwargs):
		if isinstance(d, dict):
			return {k: self.rewrite(v, *args, **kwargs) for k, v in d.items()}
		elif isinstance(d, str):
			if d.startswith(self.prefix):
				d = d[len(self.prefix):]
				if d in self.map:
					b64 = self.map[d]
					bytes = base64.b64decode(b64)
					u = uuid.UUID(bytes=bytes)
					return f'urn:uuid:{u}'
				else:
					raise Exception(f'URI does not have an assigned UUID: {d}')
			return d
		elif isinstance(d, list):
			return [self.rewrite(v, *args, **kwargs) for v in d]
		elif isinstance(d, (int, float)):
			return d
		else:
			print(f'failed to rewrite JSON value: {d!r}')
			raise Exception(f'failed to rewrite JSON value: {d!r}')

if len(sys.argv) < 2:
	cmd = sys.argv[0]
	print(f'''
Usage: {cmd} URI_PREFIX MAP_FILE_NAME

Process all json files in the output path (configured with the GETTY_PIPELINE_OUTPUT
environment variable), rewriting URIs that have the specified URI_PREFIX to urn:uuid:
URIs that are specified in the MAP_FILE_NAME JSON file.

	'''.lstrip())
	sys.exit(1)

prefix = sys.argv[1]
map_file = sys.argv[2]

print(f'Rewriting URIs to UUIDs ...')
start_time = time.time()
r = UUIDRewriter(prefix, map_file)
rewrite_output_files(r, update_filename=True, verify_uuid=True, parallel=True, ignore_errors=True)
if map_file:
	r.persist_map()
cur = time.time()
elapsed = cur - start_time
print(f'Done (%.1fs)' % (elapsed,))
