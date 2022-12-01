#!/usr/bin/env python3 -B

'''
This script converts a CSV export of the Goupil Contents Genre/Subject Sheet into JSON that
can be used as a service file in the provenance pipelines.

    ./scripts/scripts/goupil_subject_genre_concordance.py genre-subject.csv > data/goupil/subject_genre.json

[1] https://docs.google.com/spreadsheets/d/1Sls2QxeNW8UC0abZJRfSndPpJHanarkUnT5a4-J94fM/edit#gid=0
'''

from contextlib import suppress
import csv
import sys
import json
from time import sleep
import requests

subject_col = 'goupil_subject'
genre_col = 'goupil_genre'
classified_as_col = 'classified_as'
style_col = 'style'
depicts_col = 'depicts'

cache = {}

if __name__ == '__main__':
	filename = sys.argv[1]
	subject_genre = {}
	subject_genre['classified_as'] = {}
	subject_genre['represents_instance_of_type'] = {}

	subject_genre_style_col = {}

	with open(filename, 'r', encoding='utf-8-sig') as csv_file:
		reader = csv.reader(csv_file, delimiter=',')
		headers = next(reader)
		for row in reader:
			d = dict(zip(headers, row))
			subject = d.get(subject_col).strip()
			genre = d.get(genre_col).strip()
			style = d.get(style_col).strip()
			classified_as = d.get(classified_as_col).strip()
			depicts = d.get(depicts_col)
			
			if depicts:
				depicts = [d.strip() for d in depicts.strip().split(';') if depicts]
			
			if subject and genre:
				key = subject + ", " + genre
			elif subject :
				key = subject
			elif genre:
				key = genre

			classifiers = [cl.strip() for cl in classified_as.split(';') if cl]
			if not classifiers:
				continue

			if key in subject_genre['classified_as']:
				print(f"Multiple definitions for subject: '{subject}' and genre: '{genre}'", file=sys.stderr)
				continue

			subject_genre['classified_as'][key] = {}

			for cl in classifiers:
				aat = f"http://vocab.getty.edu/aat/{cl}"
				if cl in cache:
					subject_genre['classified_as'][key][cache[cl]] = aat
				else:
					url = aat + ".json"
					r = requests.get(url)
					# print(url)
					js = r.json()
					
					subject_genre['classified_as'][key][js['_label']] = aat
					cache[cl] = js['_label']
					sleep(0.1)			
			
			if not depicts:
				continue
			subject_genre['represents_instance_of_type'][key] = {}

			for d in depicts:
				aat = f"http://vocab.getty.edu/aat/{d}"
				if d in cache:
					subject_genre['represents_instance_of_type'][key][cache[d]] = aat
				else:
					url = aat + ".json"
					r = requests.get(url)
					# print(url)
					js = r.json()
					
					subject_genre['represents_instance_of_type'][key][js['_label']] = aat
					cache[d] = js['_label']
					sleep(0.1)			


	print(json.dumps(subject_genre, indent=4, sort_keys=True, ensure_ascii=False))


