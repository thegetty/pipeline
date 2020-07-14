#!/usr/bin/env python3 -B

import os
import sys
import json
import uuid
import time
import base64
import pprint
import itertools
from pathlib import Path
from contextlib import suppress
import multiprocessing

from settings import output_file_path
from pipeline.util.rewriting import rewrite_output_files

class URIFinder():
	def __init__(self, prefix, map_data):
		self.prefix = prefix
		self.map_data = map_data

	def _find_uris_walk(self, d, uris, *args, **kwargs):
		if isinstance(d, dict):
			for k, v in d.items():
				self._find_uris_walk(v, uris, *args, **kwargs)
		elif isinstance(d, str):
			if d.startswith(self.prefix):
				suffix = d[len(self.prefix):]
				if suffix not in self.map_data:
					uris.add(suffix)
		elif isinstance(d, list):
			for v in d:
				self._find_uris_walk(v, uris, *args, **kwargs)
		elif isinstance(d, (int, float)):
			pass
		else:
			print(f'failed to rewrite JSON value: {d!r}')
			raise Exception(f'failed to rewrite JSON value: {d!r}')

	def find_uris_in_file(self, f, uris=None):
		if uris is None:
			uris = set()
		with open(f) as data_file:
			try:
				data = json.load(data_file)
			except json.decoder.JSONDecodeError:
				sys.stderr.write(f'Failed to load JSON during rewriting of {f}\n')
				raise
		self._find_uris_walk(data, uris)
		return uris

def _find_uris(finder, prefix, map_data, path, files):
	uris = set()
	if path:
		for p in path.rglob('*.json'):
			finder.find_uris_in_file(p, uris)
	if files:
		for p in files:
			finder.find_uris_in_file(p, uris)
	return uris

def find_uris(prefix, map_data, path):
	'''
	Find all URIs with {prefix} in the JSON files contained in {path}.
	'''
	uris = set()
	finder = URIFinder(prefix, map_data)
	for p in [p for p in path.glob('*') if p.is_dir()]:
		args = []
		d_args = [(finder, prefix, map_data, p, []) for p in p.glob('*') if p.is_dir()]
		files = [p for p in p.glob('*.json') if p.is_file()]
		args = d_args + [(finder, prefix, map_data, None, files)]
		j = 8
		pool = multiprocessing.Pool(j)
		results = pool.starmap(_find_uris, args)
		for r in results:
			uris.update(r)
	return uris

if __name__ == '__main__':
	if len(sys.argv) < 2:
		cmd = sys.argv[0]
		print(f'''
	Usage: {cmd} URI_PREFIX MAP_FILE_NAME

	Process all JSON files in the output path (configured with the GETTY_PIPELINE_OUTPUT
	environment variable), and assign URIs that have the specified URI_PREFIX a unique
	urn:uuid URI, persisting the mapping in the MAP_FILE_NAME JSON file (which is updated
	in-place, preserving any existing data).

		'''.lstrip())
		sys.exit(1)

	prefix = sys.argv[1]
	map_file = sys.argv[2] if len(sys.argv) > 2 else None
	try:
		with open(map_file) as data_file:
			map_data = json.load(data_file)
	except FileNotFoundError:
		map_data = {}

	print(f'Generating URI to UUID map ...')
	start_time = time.time()
	uris = find_uris(prefix, map_data, Path(output_file_path))
	uuid_map = {}
	for uri in uris:
		if uri in map_data:
			raise Exception(f'URI already has a UUID set: {uri}: {map_data[uri]}')
		u = uuid.uuid4()
		bytes = base64.b64encode(u.bytes)
		b64 = bytes.decode('utf-8')
		uuid_map[uri] = b64

	map_data.update(uuid_map)
	with open(map_file, 'w') as data_file:
		json.dump(map_data, data_file)

	cur = time.time()
	elapsed = cur - start_time
	print(f'Done (%.1fs)' % (elapsed,))
