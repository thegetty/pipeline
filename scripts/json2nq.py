#!/usr/bin/env python3 -B

import sys
from pathlib import Path
import pprint
from pyld import jsonld
import json
import uuid
import re

class JSONLDError(Exception):
	pass

context_filename = sys.argv[1]
fh = open(context_filename, 'r')
ctx = json.load(fh)

proc = jsonld.JsonLdProcessor()
r = re.compile(r'_:(\S+)')

def convert(p):
	filename = str(p)
	try:
		with p.open('r') as fh:
			bnode_map = {}
			input = json.load(fh)
			try:
				id = input['id']
			except KeyError as e:
				print(f'*** Skipping {filename}')
				return 0
			if not id.startswith('urn:uuid:'):
				raise JSONLDError(f"file doesn't have a valid top-level UUID: {filename}")
			gid = id
			del(input['@context'])
			input = {'@id': gid, '@graph': input}
			triples = proc.to_rdf(input, {'expandContext': ctx, 'format': 'application/n-quads'})
	
			bids = set(r.findall(triples))
			for bid in bids:
				if bid in bnode_map:
					u = bnode_map[bid]
				else:
					u = f'b{uuid.uuid4()}'.replace('-', '')
					bnode_map[bid] = u
				triples = re.sub(f'_:{bid} ', f'_:{u} ', triples)

			nq_filename = p.with_suffix('.nq')
			with open(nq_filename, 'w') as out:
				print(triples, file=out)
				return 1
	except JSONLDError as e:
		print(f'*** {str(e)}', file=sys.stderr)
	return 0

count = 0
for filename in sys.argv[2:]:
	p = Path(filename)
	if p.is_dir():
		for p in p.rglob('*.json'):
			print(p)
			count += convert(p)
	else:
		count += convert(p)
# print(f'Done after writing {count} N-Quads files.')
