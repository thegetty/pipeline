#!/usr/bin/env python3 -B

'''
This script converts a CSV export of the City Authoriry DB JSON that
can be used as a service file in the provenance pipelines.

    ./scripts/scripts/make_cities_auth_db.py city_auth_db.csv > data/common/cities_auth_db.json
'''

from contextlib import suppress
import csv
import sys
import json
from time import sleep
import requests


if __name__ == '__main__':
	filename = sys.argv[1]

	with open(filename, 'r', encoding='utf-8-sig') as csv_file:
		reader = csv.reader(csv_file, delimiter=',')
		headers = next(reader)
		data = {}
		for row in reader:
			loc_verbatim = row[0]
			loc_name = row[1]
			loc_auth = row[2]
			
			#if location verbatim new, ignore
			if loc_verbatim == 'NEW':
				continue

			if loc_auth.startswith(loc_name):
				loc_auth = loc_auth.split(',')[-1].strip()
				
			if loc_verbatim in data:
				assert data[loc_verbatim]['name'] == loc_name
				assert data[loc_verbatim]['authority'] == loc_auth
			

			data[loc_verbatim] = {
				'name': loc_name,
				'authority': loc_auth
			}


	print(json.dumps(data, indent=4, sort_keys=True, ensure_ascii=False))


