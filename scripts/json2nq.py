#!/usr/bin/env python3 -B

import sys
from pathlib import Path
import pprint
from pyld import jsonld
import json
import uuid
import re

context_filename = sys.argv[1]
fh = open(context_filename, 'r')
ctx = json.load(fh)

proc = jsonld.JsonLdProcessor()
r = re.compile(r'_:(\S+)')

for filename in sys.argv[2:]:
	p = Path(filename)
	with open(filename, 'r') as fh:
		print(filename)
		bnode_map = {}
		input = json.load(fh)
		id = input['id']
		if not id.startswith('urn:uuid:'):
			raise Exception(f"file doesn't have a valid top-level UUID: {filename}")
		uu = id[9:]
		gid = f'http://data.getty.edu/provenance/sales/{uu}-graph'
		del(input['@context'])
		input = {'@id': gid, '@graph': input}
		triples = proc.to_rdf(input, {'expandContext': ctx, 'format': 'application/n-quads'})
		
		bids = set(r.findall(triples))
		for bid in bids:
			if bid in bnode_map:
				u = bnode_map[bid]
			else:
				u = f'b{uuid.uuid4()}'
				bnode_map[bid] = u
			triples = re.sub(f'_:{bid} ', f'_:{u} ', triples)

		nq_filename = p.with_suffix('.nq')
		with open(nq_filename, 'w') as out:
			print(triples, file=out)
			print(f'{nq_filename}')