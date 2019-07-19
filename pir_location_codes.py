#!/usr/bin/env python3 -B

'''
This will parse the HTML document of Catalog Location Codes from The Getty Provenance Index,
and create a similarly named JSON document containing the mapping from location code to
location name.
'''

import sys
from pathlib import Path
import json
import pprint
import lxml.html
from contextlib import suppress

file = Path(sys.argv[1])
with open(file, 'r') as f:
	html = '\n'.join(f.readlines())

codes = {}
root = lxml.html.document_fromstring(html)
for row in root.xpath('//table/tr/td/table/tr'):
	cells = row.xpath('./td')
	
	with suppress(IndexError):
		code = cells[1].text_content()
		name = cells[2].text_content()
		try:
			codes[code] = name
# 			print(f'{code:>16}: {name}')
		except TypeError:
			pprint.pprint([code, name])

with open(file.with_suffix('.json'), 'w') as f:
	json.dump(codes, f)
