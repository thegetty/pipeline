#!/usr/bin/env python3 -B
import unittest
import os
import os.path
import hashlib
import json
import uuid
import pprint
from pathlib import Path

from tests import TestWriter
from pipeline.projects.provenance import ProvenancePipeline
from pipeline.nodes.basic import Serializer, AddArchesModel

MODELS = {
	'Acquisition': 'model-acquisition',
	'Activity': 'model-activity',
	'Event': 'model-event',
	'Group': 'model-groups',
	'HumanMadeObject': 'model-object',
	'LinguisticObject': 'model-lo',
	'Person': 'model-person',
	'Place': 'model-place',
	'Procurement': 'model-activity',
	'Production': 'model-production',
	'Set': 'model-set',
	'VisualItem': 'model-visual-item'
}

class ProvenanceTestPipeline(ProvenancePipeline):
	'''
	Test Provenance pipeline subclass that allows using a custom Writer.
	'''
	def __init__(self, writer, input_path, catalogs, auction_events, contents, **kwargs):
		super().__init__(input_path, catalogs, auction_events, contents, **kwargs)
		self.writer = writer

	def serializer_nodes_for_model(self, *args, model=None, **kwargs):
		nodes = []
		if model:
			nodes.append(AddArchesModel(model=model))
		nodes.append(Serializer(compact=False))
		nodes.append(self.writer)
		return nodes

	def get_services(self):
		services = super().get_services()
		services.update({
			'problematic_records': {},
			'location_codes': {}
		})
		return services


class TestProvenancePipelineOutput(unittest.TestCase):
	'''
	Parse test CSV data and run the Provenance pipeline with the in-memory TestWriter.
	Then verify that the serializations in the TestWriter object are what was expected.
	'''
	def setUp(self):
		self.catalogs = {
			'header_file': 'tests/data/pir/sales_catalogs_info_0.csv',
			'files_pattern': 'tests/data/pir/empty.csv',
		}
		self.contents = {
			'header_file': 'tests/data/pir/sales_contents_0.csv',
			'files_pattern': 'tests/data/pir/empty.csv',
		}
		self.auction_events = {
			'header_file': 'tests/data/pir/sales_descriptions_0.csv',
			'files_pattern': 'tests/data/pir/empty.csv',
		}
		os.environ['QUIET'] = '1'

	def tearDown(self):
		pass

	def run_pipeline(self, test_name):
		input_path = os.getcwd()
		catalogs = self.catalogs.copy()
		events = self.auction_events.copy()
		contents = self.contents.copy()
		
		tests_path = Path(f'tests/data/pir/{test_name}')
		catalog_files = list(tests_path.rglob('sales_catalogs_info*'))
		event_files = list(tests_path.rglob('sales_descriptions*'))
		content_files = list(tests_path.rglob('sales_contents*'))
		
		if catalog_files:
			catalogs['files_pattern'] = str(tests_path / 'sales_catalogs_info*')

		if event_files:
			events['files_pattern'] = str(tests_path / 'sales_descriptions*')

		if content_files:
			contents['files_pattern'] = str(tests_path / 'sales_contents*')
		
		writer = TestWriter()
		pipeline = ProvenanceTestPipeline(
				writer,
				input_path,
				catalogs=catalogs,
				auction_events=events,
				contents=contents,
				models=MODELS,
				limit=100,
				debug=True
		)
		pipeline.run()
		return writer.processed_output()

	def test_modeling_for_destruction(self):
		'''
		All objects in this set have a 'destroyed_by' property that has type 'Destruction'.
		
		This destruction information comes from either the `lot_notes` or the
		`present_loc_geog` fields.
		'''
		output = self.run_pipeline('destruction')

		objects = output['model-object']
		self.assertEqual(len(objects), 16)
		for o in objects.values():
			self.assertIn('destroyed_by', o)
			self.assertEqual(o['destroyed_by']['type'], 'Destruction')

	def test_modeling_for_multi_artists(self):
		'''
		The object in this set has a production event that has 3 sub-events, pointing to
		the three artists modeled as people. No other people are modeled in the dataset.
		'''
		output = self.run_pipeline('multiartist')

		objects = output['model-object']
		people = output['model-person']
		self.assertEqual(len(objects), 1)
		self.assertEqual(len(people), 3)
		people_ids = set([p['id'] for p in people.values()])
		object = next(iter(objects.values()))
		event = object['produced_by']
		artists = [e['carried_out_by'][0] for e in event['part']]
		artist_ids = set([a['id'] for a in artists])
		self.assertEqual(people_ids, artist_ids)


if __name__ == '__main__':
	unittest.main()
