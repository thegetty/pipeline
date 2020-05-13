#!/usr/bin/env python3 -B

import os
import sys
from pathlib import Path
import pprint
import json
import uuid
import re

import datetime
import requests
from pyld import jsonld
from pyld.jsonld import set_document_loader

docCache = {}
def fetch(url):
	resp = requests.get(url)
	return resp.json()

def load_document_and_cache(url, *args, **kwargs):
	if url in docCache:
		return docCache[url]

	doc = {"expires": None, "contextUrl": None, "documentUrl": None, "document": ""}
	data = fetch(url)
	doc["document"] = data
	doc["expires"] = datetime.datetime.now() + datetime.timedelta(minutes=1440)
	docCache[url] = doc
	return doc

set_document_loader(load_document_and_cache)

class JSONLDError(Exception):
	pass

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

			options = {'format': 'application/n-quads'}
			if ctx:
				options['expandContext'] = ctx
				del(input['@context'])

			input = {'@id': gid, '@graph': input}
			triples = proc.to_rdf(input, options)

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
for filename in files:
	p = Path(filename)
# 	print(f'[{os.getpid()}] {filename}')
	if p.is_dir():
		for p in p.rglob('*.json'):
			print(p)
			count += convert(p)
	else:
		count += convert(p)
# print(f'Done after writing {count} N-Quads files.')
