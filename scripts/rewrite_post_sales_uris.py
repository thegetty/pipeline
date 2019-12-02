#!/usr/bin/env python3 -B

import os
import sys
import json
import uuid
import pprint
import itertools
from pathlib import Path

from settings import output_file_path
from pipeline.util.rewriting import rewrite_output_files, JSONValueRewriter
from pipeline.projects.provenance.util import prev_post_sales_rewrite_map

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
	post_sale_rewrite_map = prev_post_sales_rewrite_map(post_sale_rewrite_map)
# 	print('Post sales rewrite map:')
# 	pprint.pprint(post_sale_rewrite_map)
	r = JSONValueRewriter(post_sale_rewrite_map)
	rewrite_output_files(r, parallel=True)
print('Done')
