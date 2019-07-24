#!/usr/bin/env python3 -B

import os
import sys
import json
import uuid
import pprint
import itertools
from pathlib import Path

from settings import output_file_path
from pipeline.util.rewriting import rewrite_output_files

class UUIDRewriter:
	def __init__(self, prefix):
		self.map = {}
		self.prefix = prefix

	def rewrite(self, d, *args, **kwargs):
		if isinstance(d, dict):
			return {k: self.rewrite(v, *args, **kwargs) for k, v in d.items()}
		elif isinstance(d, list):
			return [self.rewrite(v, *args, **kwargs) for v in d]
		elif isinstance(d, int):
			return d
		elif isinstance(d, float):
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
r = UUIDRewriter(prefix)
rewrite_output_files(r, update_filename=True, verify_uuid=True)
