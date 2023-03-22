#!/usr/bin/env python3

'''
Turn the link type mapping sheet[1] into a JSON file for use as a
bonobo service file. Takes the project name and path to CSV export of the sheet
as a command line arguments and prints the converted JSON to standard out.

% link_types.py [sales] path_to_concordance.csv

[1] https://docs.google.com/spreadsheets/d/1_wDC9OpPKLJClON2LwnR-f5DtfE8S2NRfTNs40aKA94/edit#gid=449809145
'''

import os
import sys
import csv
import json
import pprint
import warnings

mapping = {}
project = sys.argv[1].lower()
if project != 'sales':
	sys.stderr.write(f'Project argument must be "sales"\n')
	sys.exit(1)

path = sys.argv[2]
with open(path, newline='') as csvfile:
	r = csv.reader(csvfile)
	iter = enumerate(r)
	_, headers = next(iter)
	for k in range(len(headers)):
		headers[k] = headers[k].strip()
	for line, row in iter:
		d = dict(zip(headers, row))
		field = d['field-name']
		if field.endswith('_N'):
			field = field[:-2]
		if 'type' in d:
			if d['type'] == 'webpage':
				d['type'] = 'WebPage'
		mapping[field] = d

print(json.dumps(mapping, indent=4))
