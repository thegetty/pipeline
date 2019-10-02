#!/usr/bin/env python3 -B
import unittest
import os
import os.path
import hashlib
import json
import uuid
import pprint

from tests import TestWriter
from pipeline.projects.provenance import ProvenancePipeline
from pipeline.nodes.basic import Serializer, AddArchesModel

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
			'files_pattern': 'tests/data/pir/sales_catalogs_info.csv',
		}
		self.contents = {
			'header_file': 'tests/data/pir/sales_contents_0.csv',
			'files_pattern': 'tests/data/pir/sales_contents_1.csv',
		}
		self.auction_events = {
			'header_file': 'tests/data/pir/sales_descriptions_0.csv',
			'files_pattern': 'tests/data/pir/sales_descriptions.csv',
		}
		os.environ['QUIET'] = '1'

	def tearDown(self):
		pass

	def run_pipeline(self, models, input_path):
		writer = TestWriter()
		pipeline = ProvenanceTestPipeline(
				writer,
				input_path,
				catalogs=self.catalogs,
				auction_events=self.auction_events,
				contents=self.contents,
				models=models,
				limit=10,
				debug=True
		)
		pipeline.run()
		return writer.processed_output()

	def verify_auction(self, a, event, idents):
		got_events = {c['_label'] for c in a.get('part_of', [])}
		self.assertEqual(got_events, {f'Auction Event for {event}'})
		got_idents = {c['content'] for c in a.get('identified_by', [])}
		self.assertEqual(got_idents, idents)

	def test_pipeline_pir(self):
		input_path = os.getcwd()
		models = {
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
			'VisualItem': 'model-visual-item'
		}
		output = self.run_pipeline(models, input_path)

		objects = output['model-object']
		events = output['model-event']
		los = output['model-lo']
		people = output['model-person']
		auctions = output['model-activity']
		groups = output['model-groups']
		AUCTION_HOUSE_TYPE = 'http://vocab.getty.edu/aat/300417515'
		houses = {k: h for k, h in groups.items()
					if h.get('classified_as', [{}])[0].get('id') == AUCTION_HOUSE_TYPE}

		self.assertEqual(len(people), 3, 'expected count of people')
		self.assertEqual(len(objects), 6, 'expected count of physical objects')
		self.assertEqual(len(los), 1, 'expected count of linguistic objects')
		self.assertEqual(len(auctions), 2, 'expected count of auctions')
		self.assertEqual(len(houses), 1, 'expected count of auction houses')
		self.assertEqual(len(events), 1, 'expected count of auction events')

		object_types = {c['_label'] for o in objects.values() for c in o.get('classified_as', [])}
		self.assertEqual(object_types, {'Auction Catalog', 'Painting'})

		lo_types = {c['_label'] for o in los.values() for c in o.get('classified_as', [])}
		self.assertEqual(lo_types, {'Auction Catalog'})

		people_names = {o['_label'] for o in people.values()}
		self.assertEqual(people_names, {'[ANONYMOUS]', 'GILLEMANS, JAN PAUWEL', 'VINCKEBOONS, DAVID'})

		key_119 = 'tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#AUCTION,B-A139,LOT,0119,DATE,1774-05-31'
		key_120 = 'tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#AUCTION,B-A139,LOT,0120,DATE,1774-05-31'

		auction_B_A139_0119 = auctions[key_119]
		self.verify_auction(auction_B_A139_0119, event='B-A139', idents={'0119[a]', '0119[b]'})

		auction_B_A139_0120 = auctions[key_120]
		self.verify_auction(auction_B_A139_0120, event='B-A139', idents={'0120'})

		house_names = {o['_label'] for o in houses.values()}
		house_ids = {o['id'] for o in houses.values()}
		house_types = {c['_label'] for o in houses.values() for c in o.get('classified_as', [])}
		self.assertEqual(house_names, {'Paul de Cock'})
		self.assertEqual(house_types, {'Auction House (organization)'})

		event_labels = {e['_label'] for e in events.values()}
		carried_out_by = {h['id'] for e in events.values() for h in e.get('carried_out_by', [])}
		self.assertEqual(event_labels, {'Auction Event for B-A139'})
		self.assertEqual(carried_out_by, house_ids)


if __name__ == '__main__':
	unittest.main()
