#!/usr/bin/env python3 -B

import os
import sys
from pathlib import Path
import pprint
from pyld import jsonld
import json
import uuid
import re

argv_i = 1
if len(sys.argv) > argv_i and sys.argv[argv_i] == '-c':
	context_filename = sys.argv[argv_i+1]
	fh = open(context_filename, 'r')
	ctx = json.load(fh)
	argv_i += 2
else:
	ctx = None

if len(sys.argv) > argv_i:
	if sys.argv[argv_i] == '-l':
		list_file = sys.argv[argv_i+1]
		with open(list_file, 'r') as fh:
			files = [f.strip() for f in fh]
	else:
		files = sys.argv[argv_i:]
else:
	files = [f.strip() for f in sys.stdin]

proc = jsonld.JsonLdProcessor()
r = re.compile(r'_:(\S+)')

count = 0
print(f'[{os.getpid()}] json2nq.py')
for filename in files:
	p = Path(filename)
	with open(filename, 'r') as fh:
		bnode_map = {}
		input = json.load(fh)

		options = {'format': 'application/n-quads'}
		if ctx:
			options['expandContext'] = ctx
			del(input['@context'])
		triples = proc.to_rdf(input, options)
		
		bids = set(r.findall(triples))
		for bid in bids:
			if bid in bnode_map:
				u = bnode_map[bid]
			else:
				u = f'b{uuid.uuid4()}'.replace('-', '')
				bnode_map[bid] = u
			triples = re.sub(f'_:{bid} ', f'_:{u} ', triples)

		with open(p.with_suffix('.nt'), 'w') as out:
			count += 1
			print(triples, file=out)
# print(f'Done after writing {count} N-Quads files.')
