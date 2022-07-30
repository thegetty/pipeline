#!/usr/bin/env python3 -B
import unittest
import os
import os.path
import hashlib
import json
import uuid
import pprint

from tests import TestWriter, SalesTestPipeline, MODELS
from cromulent import vocab

vocab.add_attribute_assignment_check()

class TestSalesPipelineOutput(unittest.TestCase):
    '''
    Parse test CSV data and run the Provenance pipeline with the in-memory TestWriter.
    Then verify that the serializations in the TestWriter object are what was expected.
    '''
    def setUp(self):
        self.catalogs = {
            'header_file': 'tests/data/sales/sales_catalogs_info_0.csv',
            'files_pattern': 'tests/data/sales/sales_catalogs_info.csv',
        }
        self.contents = {
            'header_file': 'tests/data/sales/sales_contents_0.csv',
            'files_pattern': 'tests/data/sales/sales_contents_1.csv',
        }
        self.auction_events = {
            'header_file': 'tests/data/sales/sales_descriptions_0.csv',
            'files_pattern': 'tests/data/sales/sales_descriptions.csv',
        }
        os.environ['QUIET'] = '1'

    def tearDown(self):
        pass

    def run_pipeline(self, models, input_path):
        writer = TestWriter()
        pipeline = SalesTestPipeline(
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
        self.assertEqual(got_events, {f'Auction Event {event}'})
        got_idents = {c['content'] for c in a.get('identified_by', [])}
        self.assertEqual(got_idents, idents)

    def test_pipeline_sales(self):
        input_path = os.getcwd()
        models = MODELS
        output = self.run_pipeline(models, input_path)

        objects = output['model-object']
        los = output['model-lo']
        people = output['model-person']
        prov = output['model-activity']
        activities = output['model-sale-activity']
        groups = output['model-groups']
        AUCTION_HOUSE_TYPE = 'http://vocab.getty.edu/aat/300417515'
        houses = {k: h for k, h in groups.items()
                    if h.get('classified_as', [{}])[0].get('id') == AUCTION_HOUSE_TYPE}

        self.assertEqual(len(people), 4, 'expected count of people') # 3 from the data, and 1 (Lugt) which is a static instance
        self.assertEqual(len(objects), 6, 'expected count of physical objects')
        self.assertEqual(len(los), 10, 'expected count of linguistic objects')
        self.assertEqual(len(prov), 2, 'expected count of prov entries') # 2 prov entries
        self.assertEqual(len(activities), 3, 'expected count of sale activities') # 1 auction event, 2 auctions of lot
        self.assertEqual(len(houses), 1, 'expected count of auction houses')

        object_types = {c['_label'] for o in objects.values() for c in o.get('classified_as', [])}
        self.assertEqual(object_types, {'Auction Catalog', 'Painting'})

        lo_types = {c['_label'] for o in los.values() for c in o.get('classified_as', [])}
        self.assertEqual(lo_types, {'Auction Catalog', 'Catalog', 'Entry', 'Database', 'Electronic Records'})

        people_names = {o['_label'] for o in people.values()}
        self.assertEqual(people_names, {'Frits Lugt', '[Anonymous]', 'GILLEMANS, JAN PAUWEL', 'VINCKEBOONS, DAVID'})

        key_119 = 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION,B-A139,0119,1774-05-31'
        key_120 = 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION,B-A139,0120,1774-05-31'

        auction_B_A139_0119 = activities[key_119]
        self.verify_auction(auction_B_A139_0119, event='B-A139 (1774-05-31 onwards)', idents={'0119[a]', '0119[b]', 'Auction of Lot B-A139 0119 (1774-05-31)'})

        auction_B_A139_0120 = activities[key_120]
        self.verify_auction(auction_B_A139_0120, event='B-A139 (1774-05-31 onwards)', idents={'0120', 'Auction of Lot B-A139 0120 (1774-05-31)'})

        house_ids = {o['id'] for o in houses.values()}
        house_types = {c['_label'] for o in houses.values() for c in o.get('classified_as', [])}
        self.assertEqual(house_types, {'Auction House (organization)'})

        offer_labels = {p['_label'] for p in prov.values()}
        self.assertEqual(offer_labels, {'Offer of B-A139 0119 (1774-05-31)', 'Offer of B-A139 0120 (1774-05-31)'})

        events = [activities[k] for k in activities if k not in {key_119, key_120}]
        event_labels = {e['_label'] for e in events}
        self.assertEqual(event_labels, {'Auction Event B-A139 (1774-05-31 onwards)'})


if __name__ == '__main__':
    unittest.main()
