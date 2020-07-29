#!/usr/bin/env python3 -B

'''
This script converts a CSV export of the GRI Place Names Concordance[1] into JSON that
can be used as a service file in the provenance pipelines.

    ./scripts/parse_gri_place_concordance.py place_concordance_v5.csv > data/common/unique_locations.json

[1] https://drive.google.com/drive/folders/18flvM28zXhDMSmRsrdbOtewvdldyZ2qQ
'''

import os
import csv
import sys
import json
import pprint
from contextlib import suppress

place_keys = {
	'CITY/TOWN': 'city',
	'COUNTRY': 'country',
	'SOVERIGN': 'sovereign',
	'STATE/PROVINCE': 'state'
}

if __name__ == '__main__':
	filename = sys.argv[1]
	places = {}
	names = {}
	with open(filename, 'r', encoding='utf-8-sig') as csv_file:
		reader = csv.reader(csv_file, delimiter=',')
		headers = next(reader)
		
		for row in reader:
			d = dict(zip(headers, row))
			authname = d.get('AUTHORITY')
			city = d.get('CITY/TOWN')
			country = d.get('COUNTRY')
			sovereign = d.get('SOVERIGN')
			state = d.get('STATE/PROVINCE')
			verbatims = [d.get(f'VERBATIM_{i}') for i in range(1,7)]
			for verbatim in verbatims:
				if verbatim:
					names[verbatim] = authname
			
			place = {}
			for k in d.keys():
				if k in place_keys:
					key = place_keys[k]
					value = d[k]
					if value:
						place[key] = value
			if place.get('sovereign') == 'UK' or place.get('country') == 'Ireland':
				with suppress(KeyError):
					county = place['state']
					del place['state']
					place['county'] = county
			if place:
				places[authname] = place
	data = {'places': places, 'canonical_names': names}
	print(json.dumps(data, indent=4, sort_keys=True))
