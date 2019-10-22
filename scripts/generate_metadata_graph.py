 #!/usr/bin/env python3 -B

import sys
from pathlib import Path
import pprint
from pyld import jsonld
import json
import uuid
import re
import warnings
from collections import defaultdict

from settings import output_file_path, arches_models

project_name = sys.argv[1]
models = {v: k for k, v in arches_models.items()}

proc = jsonld.JsonLdProcessor()
files = sorted(Path(output_file_path).rglob('*.json'))
uuid_re = re.compile('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}.json')

graphs = defaultdict(list)
for filename in files:
	p = Path(filename)
	m = uuid_re.match(p.name)
	if not m:
		continue
	uu = p.name[:36]
	gid = f'urn:uuid:{uu}'

	model = models.get(p.parent.parent.name)
	if not model:
		warnings.warn(f'Not a valid model: {model}')
	graphs[model].append(gid)


ctx = {
	'@vocab': 'http://example.org/',
	'model': {'@type': '@id'},
	'models': {
		'@type': '@id',
		'@id': 'model',
		'@container': '@index',
		# TODO: this is currently not supported in pyld, but should be used to produce the expected output when support is added:
		# '@index': 'type'
	}
}

data = {
	'@context': ctx,
	'@id': f'http://data.getty.edu/provenance/{project_name}/metadata',
	'@graph': {
		'@id': '_:b',
		'models': graphs
	}
}

jld_filename = Path(output_file_path).joinpath('meta.json')
with open(jld_filename, 'w') as out:
	json.dump(data, out)

nq_filename = Path(output_file_path).joinpath('meta.nq')
triples = proc.to_rdf(data, {'format': 'application/n-quads'})
with open(nq_filename, 'w') as out:
	print(triples, file=out)
