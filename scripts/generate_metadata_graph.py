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

if __name__ == '__main__':
	project_name = sys.argv[1]
	models = {v: k for k, v in arches_models.items()}

	proc = jsonld.JsonLdProcessor()
	uuid_re = re.compile('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}.json')

	graphs = defaultdict(list)
	for line in sys.stdin:
		filename = line.rstrip()
		p = Path(filename)
		m = uuid_re.match(p.name)
		if not m:
			continue
		uu = p.name[:36]
		gid = f'urn:uuid:{uu}'

		model = models.get(p.parent.parent.name)
		if not model:
			warnings.warn(f'Not a valid model for {filename}: {model}')
			continue
		graphs[model].append(gid)


	ctx = {
		'@vocab': 'http://data.getty.edu/provenance/models/',
	## TODO: the combination of @container and @index is currently not supported in pyld,
	##       but should be used to produce the expected output when support is added
		'models': {
			'@id': 'http://data.getty.edu/p/models',
			'@type': '@id',
	# 		'@id': 'model',
	# 		'@container': '@index',
	# 		# '@index': 'type',
		},
	# 	'model': {'@type': '@id'},
		'Acquisition': { '@type': '@id' },
		'Activity': { '@type': '@id' },
		'Destruction': { '@type': '@id' },
		'Event': { '@type': '@id' },
		'Group': { '@type': '@id' },
		'HumanMadeObject': { '@type': '@id' },
		'LinguisticObject': { '@type': '@id' },
		'Organization': { '@type': '@id' },
		'Person': { '@type': '@id' },
		'Phase': { '@type': '@id' },
		'Place': { '@type': '@id' },
		'Procurement': { '@type': '@id' },
		'VisualItem': { '@type': '@id' },
	}

	data = {
		'@context': ctx,
		'@id': f'http://data.getty.edu/provenance/{project_name}',
		'@graph': {
			'@id': f'http://data.getty.edu/provenance/{project_name}/dataset',
			'models': {
				'@id': f'http://data.getty.edu/provenance/{project_name}/dataset/models',
				**graphs
			}
		}
	}

	jld_filename = Path(output_file_path).joinpath('meta.json')
	with open(jld_filename, 'w') as out:
		json.dump(data, out)

	nq_filename = Path(output_file_path).joinpath('meta.nq')
	# TODO: this is too slow to work on the full dataset, so we manually construct the nq data
	# triples = proc.to_rdf(data, {'format': 'application/n-quads'})
	# with open(nq_filename, 'w') as out:
	# 	print(triples, file=out)

	with open(nq_filename, 'w') as out:
		print(f'<http://data.getty.edu/provenance/{project_name}/dataset> <http://data.getty.edu/p/models> <http://data.getty.edu/provenance/{project_name}/dataset/models> <http://data.getty.edu/provenance/{project_name}> .', file=out)
		for model, graphs in graphs.items():
			for gid in graphs:
				print(f'<http://data.getty.edu/provenance/{project_name}/dataset/models> <http://data.getty.edu/provenance/models/{model}> <{gid}> <http://data.getty.edu/provenance/{project_name}> .', file=out)
