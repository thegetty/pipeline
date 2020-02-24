#!/usr/bin/env python3 -B

import unittest
import os
import pprint
from collections import defaultdict
from contextlib import suppress
import hashlib
import json
import uuid

from cromulent import vocab
from tests import TestWriter
from pipeline.util import CromObjectMerger
from pipeline.projects.aata import AATAPipeline
from pipeline.nodes.basic import Serializer, AddArchesModel

def merge_lists(l, r):
	'''
	Given two lists `l` and `r`, return a generator of the combined items from both lists.

	If any two items l' in l and r' in r are both `dict`s and have the same value for the
	`id` key, they will be `merge`d and the resulting `dict` included in the results in
	place of l' or r'.
	'''
	identified = {}
	all_items = l + r
	other = []
	for item in all_items:
		try:
			item_id = item['id']
			if item_id in identified:
				identified[item_id] += [item]
			else:
				identified[item_id] = [item]
		except:
			other.append(item)

	for ident, items in identified.items():
		r = items[:]
		while len(r) > 1:
			a = r.pop(0)
			b = r.pop(0)
			m = merge(a, b)
			r.insert(0, m)
		yield from r

	yield from other

def merge(l, r):
	'''
	Given two items `l` and `r` of the same type, merge their contents and return the
	result. Raise an exception if `l` and `r` are of differing types.

	If the items are of type `dict`, recursively merge any values with shared keys, and
	also include data from any non-shared keys. If `l` and `r` both have values for the
	`id` key and they differ in value, raise an exception.

	If the items are of type `list`, merge them with `merge_lists`.

	If the items are of type `str` or `bytes`, return the value if `l` and `r` are equal.
	Otherwise raise and exception.
	'''
	if l is None:
		return r
	if r is None:
		return l

	if not isinstance(l, type(r)):
		pprint.pprint(l)
		pprint.pprint(r)
		raise Exception('bad data in json merge')

	if isinstance(l, dict):
		keys = set(list(l.keys()) + list(r.keys()))
		intersection = {k for k in keys if k in l and k in r}
		if 'id' in intersection:
			lid = l['id']
			rid = r['id']
			if lid != rid:
				pprint.pprint(l)
				pprint.pprint(r)
				raise Exception('attempt to merge two dicts with different ids: (%r, %r)' % (lid, rid))
		return {k: merge(l.get(k), r.get(k)) for k in keys}
	elif isinstance(l, list):
		return list(merge_lists(l, r))
	elif isinstance(l, (str, bytes)):
		if l == r:
			return l
		else:
			raise Exception('data conflict: %r <=> %r' % (l, r))
	else:
		raise NotImplementedError('unhandled type: %r' % (type(l)))
	return l

class AATATestPipeline(AATAPipeline):
	'''
	Test AATA pipeline subclass that allows using a custom Writer.
	'''
	def __init__(self, writer, input_path, abstracts_pattern, journals_pattern, series_pattern, **kwargs):
		super().__init__(input_path, abstracts_pattern, journals_pattern, series_pattern, **kwargs)
		self.writer = writer
	
	def serializer_nodes_for_model(self, model=None):
		nodes = []
		if model:
			nodes.append(AddArchesModel(model=model))
		nodes.append(Serializer(compact=False))
		nodes.append(self.writer)
		return nodes

	def get_services(self):
		services = super().get_services()
		services.update({
			'language_code_map': {
				"eng": "english",
			},
			'document_types': {
				"AV": "AudioVisualContent",
				"BA": "Chapter",
				"BC": "Monograph",
				"BM": "Monograph",
				"JA": "Article",
				"JW": "Issue",
				"PA": "Patent",
				"TH": "Thesis",
				"TR": "TechnicalReport"
			}
		})
		return services

	def run(self):
		super().run()
		return self.writer.processed_output()

class TestAATAPipelineOutput_Abstracts(unittest.TestCase):
	'''
	Parse test XML data and run the AATA pipeline with the in-memory TestWriter.
	Then verify that the serializations in the TestWriter object are what was expected.
	'''
	def setUp(self):
		self.abstracts_pattern = 'tests/data/aata-sample1-abstracts.xml'
		self.journals_pattern = None
		self.series_pattern = None
		os.environ['QUIET'] = '1'

	def run_pipeline(self, models, input_path):
		vocab.add_linked_art_boundary_check()
		vocab.add_attribute_assignment_check()
		writer = TestWriter()
		pipeline = AATATestPipeline(
			writer,
			input_path,
			self.abstracts_pattern,
			self.journals_pattern,
			self.series_pattern,
			models=models,
			limit=1,
		)
		return pipeline.run()

	def verify_people_for_AATA140375(self, output, people_model):
		people = output[people_model].values()
		people_creation_events = set()
		for p in people:
			for event in p.get('carried_out', []):
				cid = event['id']
				people_creation_events.add(cid)
		people_names = sorted(p.get('_label') for p in people)
		self.assertEqual(people_names, ['Bremner, Ian', 'Meyers, Eric'])
		return people_creation_events

	def verify_properties_AATA140375(self, data):
		abstract, article = data.values()
		article_classification = {l['_label'] for l in article['classified_as']}
		if 'Abstract' in article_classification:
			abstract, article = article, abstract
		
		self.assertIn('The Forbidden City in Beijing', abstract['content'])
		self.assertEqual('http://vocab.getty.edu/aat/300026032', abstract['classified_as'][0]['id']) # abstract
		self.assertEqual('AATA140375', abstract['identified_by'][0]['content'])
		self.assertEqual('Owner-Assigned Number', abstract['identified_by'][0]['classified_as'][0]['_label'])
		self.assertEqual('English', abstract['language'][0]['_label'])
		self.assertEqual('LinguisticObject', abstract['type'])
		
		abstracted_thing = abstract['refers_to'][0]
		abstracted_thing_id = abstracted_thing.get('id')
		article_id = article.get('id')
		self.assertEqual(article_id, abstracted_thing_id, 'Article and the abstracgted thing have the same ID')

		merged_thing = merge(article, abstracted_thing)
		self.assertIn('Secrets of the Forbidden City', merged_thing['_label'])
		self.assertEqual('http://vocab.getty.edu/aat/300028045', merged_thing['classified_as'][0]['id']) # AV
		self.assertEqual('LinguisticObject', merged_thing['type'])
		self.assertEqual('Creation', merged_thing['created_by']['type'])
		identifiers = defaultdict(set)
		for x in merged_thing['identified_by']:
			identifiers[x['classified_as'][0]['_label']].add(x['content'])
		self.assertEqual(dict(identifiers), {
			'Title': {'Secrets of the Forbidden City'},
			'ISBN Identifier': {'1531703461', '9781531703462'},
			'Owner-Assigned Number': {'AATA140375'},
		})

		about = defaultdict(set)
		for x in merged_thing['about']:
			about[x['type']].add(x['_label'])
		self.assertEqual(about, {
			'Group': {'Palace Museum //Beijing (China)'},
			'Type': {
				'Ming',
				'Structural studies and consolidation of buildings',
				'brackets (structural elements)',
				'building materials',
				'construction techniques',
				'earthquakes',
				'experimentation',
				'historic structures (single built works)',
				'seismic design',
				'structural analysis'
			}
		})

	def verify_model_counts_for_AATA140375(self, output, lo_model, people_model, orgs_model):
		expected_models = {
			people_model,
			lo_model,
			orgs_model
		}
		self.assertEqual(set(output.keys()), expected_models)
		self.assertEqual(len(output[people_model]), 2)
		self.assertEqual(len(output[lo_model]), 2)
		self.assertEqual(len(output[orgs_model]), 3)

	def verify_organizations_for_AATA140375(self, output, orgs_model):
		organizations = output[orgs_model].values()
		org_names = {}
		for o in organizations:
			try:
				i = o['id']
				l = o.get('_label')
				org_names[i] = l
			except Exception as e:
				print('*** %s' % (e,))
				org_names[i] = None
		self.assertEqual(sorted(org_names.values()), [
			'Lion Television USA //New York (New York, United States)',
			'Public Broadcasting Associates, Inc. //Boston (Massachusetts, United States)',
			'WGBH Educational Foundation //Boston (Massachusetts, United States)'
		])

	def verify_data_for_AATA140375(self, output, lo_model):
		lo = output[lo_model].values()
		article_types = {}
		creation_labels = set()
		for a in lo:
			i = a['id']
			try:
				article_types[i] = a['classified_as'][0]['_label']
			except Exception as e:
				print('*** error while handling linguistic object classification: %s' % (e,))
				article_types[i] = None
			try:
				if 'created_by' in a:
					if a['classified_as'][0]['_label'] == 'Abstract':
						c = a['created_by']
						for p in c.get('part', []):
							creation_labels.add(p['_label'])
				for thing in a.get('refers_to', []):
					if 'created_by' in thing:
						event = thing['created_by']
						for p in event.get('part', []):
							creation_labels.add(p['_label'])
			except Exception as e:
				print('*** error while handling creation event: %s' % (e,))
				pprint.pprint(c)
		self.assertEqual(creation_labels, {
			'Creation sub-event for Producer by “Bremner, Ian”',
			'Creation sub-event for Narrator by “Meyers, Eric”',
			'Creation sub-event for Director by “Bremner, Ian”'
		})
		types = sorted(article_types.values())
		self.assertEqual(types, ['A/V Content', 'Abstract'])
		self.verify_properties_AATA140375(output[lo_model])

	def test_pipeline_with_AATA140375(self):
		input_path = os.getcwd()
		models = {
			'Person': 'model-person',
			'LinguisticObject': 'model-lo',
			'Group': 'model-groups',
			'Journal': 'model-journal',
			'Series': 'model-series',
		}
		output = self.run_pipeline(models, input_path)
		self.assertEqual(len(output), 3)

		lo_model = models['LinguisticObject']
		people_model = models['Person']
		orgs_model = models['Group']

		self.verify_model_counts_for_AATA140375(output, lo_model, people_model, orgs_model)
		people_creation_events = self.verify_people_for_AATA140375(output, people_model)
		self.verify_organizations_for_AATA140375(output, orgs_model)
		self.verify_data_for_AATA140375(output, lo_model)

class TestAATAPipelineOutput_Journals(unittest.TestCase):
	def setUp(self):
		self.maxDiff = None
		self.abstracts_pattern = None
		self.journals_pattern = 'tests/data/aata-sample1-journals.xml'
		self.series_pattern = None
		os.environ['QUIET'] = '1'

	def run_pipeline(self, models, input_path):
		writer = TestWriter()
		pipeline = AATATestPipeline(
			writer,
			input_path,
			self.abstracts_pattern,
			self.journals_pattern,
			self.series_pattern,
			models=models,
			limit=1,
		)
		return pipeline.run()

	def test_pipeline_with_AATA140375(self):
		input_path = os.getcwd()
		models = {
			'Person': 'model-person',
			'LinguisticObject': 'model-lo',
			'Group': 'model-org',
			'Journal': 'model-journal',
			'Series': 'model-series',
		}
		output = self.run_pipeline(models, input_path)
		self.assertEqual(len(output), 2)

		lo_model = models['LinguisticObject']
		journal_model = models['Journal']
		
		TAG_PREFIX = 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:aata#'
		
		journal = output[journal_model].get(f'{TAG_PREFIX}AATA,Journal,2')
		issue = output[lo_model].get(f'{TAG_PREFIX}AATA,Journal,2,Issue,9')
		
		self.assertIsNotNone(journal)
		self.assertIsNotNone(issue)
		
		for i in journal['identified_by']:
			with suppress(KeyError):
				del i['id'] # remove UUIDs that will not be stable across runs
		
		journal_expected = {
			'@context': 'https://linked.art/ns/v1/linked-art.json',
			'_label': 'Green chemistry',
			'classified_as': [
				{
					'_label': 'Journal',
					'id': 'http://vocab.getty.edu/aat/300215390',
					'type': 'Type'
				}
			],
			'id': 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:aata#AATA,Journal,2',
			'identified_by': [
				{
					'classified_as': [
						{
							'_label': 'ISSN Identifier',
							'id': 'http://vocab.getty.edu/aat/300417430',
							'type': 'Type'
						}
					],
					'content': '1463-9262',
					'type': 'Identifier'
				},
				{
					'content': 'Green chemistry: an international journal and green chemistry resource',
					'type': 'Identifier'
				},
				{
					'classified_as': [
						{
							'_label': 'Title',
							'id': 'http://vocab.getty.edu/aat/300417193',
							'type': 'Type'
						}
					],
					'content': 'Green chemistry',
					'type': 'Name'
				}
			],
			'type': 'LinguisticObject'
		}
		self.assertEqual(journal, journal_expected)
		
		# stitch together the part-of hierarchy
		parts = []
		for p in issue.get('part_of', []):
			i = p['id']
			part = None
			for k, model in output.items():
				if i in model:
					d = model[i].copy()
					del d['@context']
					part = d
					break
			if part:
				parts.append(part)
			else:
				parts.append(p)
# 				print(f'*** Did not find id {i}')
		issue['part_of'] = parts

		for i in issue['identified_by'] + issue['referred_to_by']:
			with suppress(KeyError):
				del i['id'] # remove UUIDs that will not be stable across runs
		issue_expected = {
			'@context': 'https://linked.art/ns/v1/linked-art.json',
			'_label': 'Issue 9 of “Green chemistry”',
			'classified_as': [
				{
					'_label': 'Issue',
					'id': 'http://vocab.getty.edu/aat/300312349',
					'type': 'Type'
				}
			],
			'id': 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:aata#AATA,Journal,2,Issue,9',
			'identified_by': [
				{
					'classified_as': [
						{
						'_label': 'Issue',
						'id': 'http://vocab.getty.edu/aat/300312349',
						'type': 'Type'
						}
					],
					'content': '9',
					'type': 'Identifier'
				},
				{
					'classified_as': [
						{
							'_label': 'Title',
							'id': 'http://vocab.getty.edu/aat/300417193',
							'type': 'Type'
						}
					],
					'content': 'Issue 9 of “Green chemistry”',
					'type': 'Name'
				}
			],
			'part_of': [
				{
					'_label': 'Green chemistry',
					'classified_as': [
						{
							'_label': 'Journal',
							'id': 'http://vocab.getty.edu/aat/300215390',
							'type': 'Type'
						}
					],
					'id': 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:aata#AATA,Journal,2',
					'identified_by': [
						{
							'classified_as': [
								{
								'_label': 'ISSN Identifier',
								'id': 'http://vocab.getty.edu/aat/300417430',
								'type': 'Type'
								}
							],
							'content': '1463-9262',
							'type': 'Identifier'
						},
						{
							'content': 'Green chemistry: an international journal and green chemistry resource',
							'type': 'Identifier'
						},
						{
							'classified_as': [
								{
									'_label': 'Title',
									'id': 'http://vocab.getty.edu/aat/300417193',
									'type': 'Type'
								}
							],
							'content': 'Green chemistry',
							'type': 'Name'
						}
					],
					'type': 'LinguisticObject'
				},
				{
					'_label': 'Volume 9 of “Green chemistry”',
					'id': 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:aata#AATA,Journal,2,Volume,9',
					'type': 'LinguisticObject'
				}
			],
			'referred_to_by': [
				{
					'classified_as': [
						{
							'_label': 'Note',
							'classified_as': [
								{
									'_label': 'Brief Text',
									'id': 'http://vocab.getty.edu/aat/300418049',
									'type': 'Type'
								}
							],
							'id': 'http://vocab.getty.edu/aat/300027200',
							'type': 'Type'
						},
					],
					'content': 'v. 24 (2005)',
					'type': 'LinguisticObject'
				}
			],
			'type': 'LinguisticObject'
 		}
		self.assertEqual(issue, issue_expected)


if __name__ == '__main__':
	unittest.main()
