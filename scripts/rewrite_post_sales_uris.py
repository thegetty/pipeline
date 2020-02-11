#!/usr/bin/env python3 -B

import re
import os
import sys
import json
import uuid
import pprint
import itertools
from pathlib import Path

from settings import output_file_path
from pipeline.util.rewriting import rewrite_output_files, JSONValueRewriter

if len(sys.argv) < 2:
	cmd = sys.argv[0]
	print(f'''
Usage: {cmd} URI_REWRITE_MAP.json

	'''.lstrip())
	sys.exit(1)

print(f'Rewriting post-sales URIs ...')
rewrite_map_filename = sys.argv[1]
with open(rewrite_map_filename, 'r') as f:
	post_sale_rewrite_map = json.load(f)
# 	print('Post sales rewrite map:')
# 	pprint.pprint(post_sale_rewrite_map)
	r = JSONValueRewriter(post_sale_rewrite_map, prefix=True)

	kwargs = {}
	prefix = os.path.commonprefix(list(post_sale_rewrite_map.keys()))
	if len(prefix) > 20:
		kwargs['content_filter_re'] = re.compile(re.escape(prefix))
	rewrite_output_files(r, parallel=True, **kwargs)
print('Done')
