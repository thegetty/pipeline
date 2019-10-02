#!/usr/bin/env python3 -B

'''
Look at all JSON files in a specified folder. For any that share the value
of the top-level 'id' key, use `pipeline.util.CromObjectMerger` to merge
the data, writing the result to the first seen file, and removing the
second file.
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


path = sys.argv[1] if len(sys.argv) > 1 else output_file_path
files = sorted(Path(path).rglob('*.json'))
seen = {}

read = reader.Reader()
coalesce_count = 0
print(f'Coalescing JSON files in {path} ...')
counter = Counter()
files_by_id = defaultdict(list)
for filename in files:
	p = Path(filename)
	id = p.name
	counter[id] += 1
	files_by_id[id].append(p)

for id in sorted(counter):
	count = counter[id]
	if count > 1:
		files = files_by_id[id]
		for filename in files:
			with open(filename, 'r') as fh:
				content = fh.read()
				try:
					m = read.read(content)
					id = m.id
					if id in seen:
						canon_file = seen[id]
		# 				print(f'*** {id} already seen in {canon_file} ; merging {filename}')
						merger = CromObjectMerger()
						with open(canon_file, 'r') as cfh:
							canon_content = cfh.read()
							n = read.read(canon_content)
							try:
								merger.merge(m, n)
							except model.DataError:
								print(f'Exception caught while merging data from {newfile}:')
								print(d)
								print(content)
								raise
						merged_data = factory.toString(m, False)
						d = json.loads(merged_data)
						with open(canon_file, 'w') as data_file:
							json.dump(d, data_file, indent=2, ensure_ascii=False)
							os.remove(filename)
						coalesce_count += 1
					else:
						seen[id] = filename
				except model.DataError as e:
					print(f'*** Failed to read CRM data from {filename}: {e}')
print(f'Coalesced {coalesce_count} JSON files in {path}')
