#!/usr/bin/env python3 -B

import unittest
import os
import pprint
from collections import defaultdict
from contextlib import suppress
import hashlib
import json
import uuid

from tests import TestWriter, merge
from pipeline.projects.aata import AATAPipeline
from pipeline.nodes.basic import Serializer, AddArchesModel

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
		self.assertEqual('Local Number', abstract['identified_by'][0]['classified_as'][0]['_label'])
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
			'Local Number': {'AATA140375'},
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
			'Organization': 'model-org',
			'Journal': 'model-journal',
			'Series': 'model-series',
		}
		output = self.run_pipeline(models, input_path)
		self.assertEqual(len(output), 3)

		lo_model = models['LinguisticObject']
		people_model = models['Person']
		orgs_model = models['Organization']

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
			'Organization': 'model-org',
			'Journal': 'model-journal',
			'Series': 'model-series',
		}
		output = self.run_pipeline(models, input_path)
		self.assertEqual(len(output), 2)

		lo_model = models['LinguisticObject']
		journal_model = models['Journal']
		
		TAG_PREFIX = 'tag:getty.edu,2019:digital:pipeline:aata:REPLACE-WITH-UUID#'
		
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
			'id': 'tag:getty.edu,2019:digital:pipeline:aata:REPLACE-WITH-UUID#AATA,Journal,2',
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
		
		for i in [i for p in issue['part_of'] for i in p['identified_by']]:
			with suppress(KeyError):
				del i['id'] # remove UUIDs that will not be stable across runs
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
			'id': 'tag:getty.edu,2019:digital:pipeline:aata:REPLACE-WITH-UUID#AATA,Journal,2,Issue,9',
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
					'id': 'tag:getty.edu,2019:digital:pipeline:aata:REPLACE-WITH-UUID#AATA,Journal,2',
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
					'classified_as': [
						{
							'_label': 'Volume',
							'id': 'http://vocab.getty.edu/aat/300265632',
							'type': 'Type'
						}
					],
					'id': 'tag:getty.edu,2019:digital:pipeline:aata:REPLACE-WITH-UUID#AATA,Journal,2,Volume,9',
					'identified_by': [
						{
							'classified_as': [
								{
									'_label': 'Volume',
									'id': 'http://vocab.getty.edu/aat/300265632',
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
							'content': 'Volume 9 of “Green chemistry”',
							'type': 'Name'
						}
					],
					'type': 'LinguisticObject'
				}
			],
			'referred_to_by': [
				{
					'classified_as': [
						{
							'_label': 'Note',
							'id': 'http://vocab.getty.edu/aat/300027200',
							'type': 'Type'
						},
						{
							'_label': 'Brief Text',
							'id': 'http://vocab.getty.edu/aat/300418049',
							'type': 'Type'
						}
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
