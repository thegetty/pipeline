#!/usr/bin/env python3 -B

'''
For every JSON file identified in ARGV, ensure that it is located in the
correct directory structure. If it isn't, move it to the correct directory.
'''

import re
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

### This script avoids these status messages, because a normal pipeline run will invoke
### this script thousands of times in parallel.
# print('Reorganizing JSON files...')
uuid_re = re.compile('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}.json')
for filename in files:
	p = Path(filename)
	m = uuid_re.match(p.name)
	if not m:
		continue
	correct_partition = p.name[0:2]
	if len(p.parent.name) != 2:
		raise Exception(f"file does not appear to be in a 1-byte partition directory: {p.parent.name}")
	correct_path = p.parent.parent.joinpath(correct_partition)
	correct_filename = correct_path.joinpath(p.name)
	if p != correct_filename:
		p.replace(correct_filename)
# 		print(f'mv {p} {correct_filename}')
	else:
		pass
# 		print(f'Already in the correct directory: {p}')
# print('Done')
