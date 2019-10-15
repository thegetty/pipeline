#!/usr/bin/env python3 -B

'''
For every JSON file identified in ARGV, ensure that it is located in the
correct directory structure. If it isn't, move it to the correct directory.
'''

import sys
import json
from pathlib import Path

from settings import output_file_path

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
		data = json.load(fh)
		try:
			uu = data['id']
		except KeyError as e:
			print(f'*** ERROR:{filename}: {e}')
			continue
		if not uu.startswith('urn:uuid:'):
			raise Exception(f"file doesn't have a valid top-level UUID: {filename}")
		correct_partition = uu[9:11]
		if len(p.parent.name) != 2:
			raise Exception(f"file does not appear to be in a 1-byte partition directory: {p.parent.name}")
		correct_path = p.parent.parent.joinpath(correct_partition)
		correct_filename = correct_path.joinpath(p.name)
		if p != correct_filename:
			p.replace(correct_filename)
# 			print(f'mv {p} {correct_filename}')
		else:
			pass
# 			print(f'ALREADY CORRECT: {p}')
