#!/usr/bin/env python3 -B

'''
This script converts a CSV export of the GRI Materials[1, 2] into JSON that
can be used as a service file in the provenance pipelines.

    ./scripts/parse_gri_currencies.py data/common/materials_map.csv > data/common/materials_map.json

[1] https://github.com/thegetty/pipeline/blob/master/data/common/materials_map.csv
[2] https://docs.google.com/spreadsheets/d/1V9PvuUchH2lZUSUrHp6I2T4QVWPXJNWK2WNIGsZ1AVo/edit#gid=0

'''

import os
import csv
import sys
import json
import pprint
from contextlib import suppress

if __name__ == '__main__':
	filename = sys.argv[1]
	materials = []
	with open(filename, 'r', encoding='utf-8-sig') as csv_file:
		reader = csv.reader(csv_file, delimiter=',')
		headers = next(reader)
		value_columns = set(headers[3:])
		
		for row in reader:
			d = dict(zip(headers, row))
			values = {k:v for k,v in d.items() if k in value_columns and v}
			if not values:
				continue
			with suppress(KeyError):
				del d['']
			materials.append(d)
	print(json.dumps(materials, indent=4, sort_keys=True))
