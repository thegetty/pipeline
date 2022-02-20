#!/usr/bin/env python3

'''
Turn the transaction type mapping sheet[1] into a JSON file for use as a
bonobo service file. Takes the project name and path to CSV export of the sheet
as a command line arguments and prints the converted JSON to standard out.

% transaction_classification.py [knoedler|sales] path_to_concordance.csv

[1] https://docs.google.com/spreadsheets/d/1dNYYy7gkWn_pT-w0lZz3fNVmjzPlVJ6kXz1nOOodyRI/edit#gid=0
'''

import os
import sys
import csv
import json
import pprint
import warnings

IGNORE = 'n/a'
TEMP = 'use a temp'

mapping = {}
project = sys.argv[1].capitalize()
path = sys.argv[2]
with open(path, newline='') as csvfile:
	r = csv.reader(csvfile)
	iter = enumerate(r)
	_, headers = next(iter)
	for k in range(len(headers)):
		headers[k] = headers[k].strip()
	for line, row in iter:
		d = dict(zip(headers, row))
		if project not in d:
			print(f'*** {project} not in data:')
			pprint.pprint(d)
			sys.exit(1)
		t = d[project]
		if t != IGNORE:
			for tt in t.split('/'):
				url = d['AAT ID']
				label = d['AAT']
				if url == TEMP:
					url = f'http://vocab.getty.edu/aat/XXX-TX-{line}'
				entry = {'url': url}
				if label != IGNORE:
					entry['label'] = label
				mapping[tt] = entry

print(json.dumps(mapping, indent=4))
