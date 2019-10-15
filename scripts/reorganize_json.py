#!/usr/bin/env python3 -B

'''
For every JSON file identified in ARGV, ensure that it is located in the
correct directory structure. If it isn't, move it to the correct directory.
'''

import os
import csv
import sys
import json
import uuid
import pprint
import itertools
from pathlib import Path
from collections import defaultdict, Counter

from settings import output_file_path
from pipeline.util import CromObjectMerger
from cromulent.model import factory
from cromulent import model, reader

files = []
if len(sys.argv) > 1:
	for p in sys.argv[1:]:
		path = Path(p)
		if path.is_dir():
			files += sorted(str(s) for s in path.rglob('*.json'))
		else:
			files.append(p)
else:
	files = sorted(Path(output_file_path).rglob('*.json'))

for filename in files:
	p = Path(filename)
	with open(filename, 'r') as fh:
		input = json.load(fh)
		try:
			id = input['id']
		except KeyError as e:
			print(f'*** ERROR:{filename}: {e}')
			continue
		if not id.startswith('urn:uuid:'):
			raise Exception(f"file doesn't have a valid top-level UUID: {filename}")
		correct_partition = id[9:11]
		if len(p.parent.name) != 2:
			raise Exception(f"file does not appear to be in a 1-byte partition directory: {p.parent.name}")
		correct_path = p.parent.parent.joinpath(correct_partition)
		correct_filename = correct_path.joinpath(p.name)
		if p != correct_filename:
			p.replace(correct_filename)
# 			print(f'{p} ---> {correct_filename}')
		else:
			pass
# 			print(f'ALREADY CORRECT: {p}')
