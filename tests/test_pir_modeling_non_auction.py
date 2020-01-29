#!/usr/bin/env python3 -B
import unittest
import os
import os.path
import hashlib
import json
import uuid
import pprint
import inspect
from itertools import groupby
from pathlib import Path
import warnings

from tests import TestProvenancePipelineOutput
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_NonAuctionSales(TestProvenancePipelineOutput):
	def test_modeling_non_auction_sales(self):
		'''
		Test for modeling of sales that are not auctions.
		'''
		output = self.run_pipeline('non_auction')
		self.verify_catalogs(output)
		self.verify_sales(output)
	
	def verify_catalogs(self, output):
		objects = output['model-object']
		activities = output['model-activity']
		texts = output['model-lo']

		expected_catalog_text_id = 'tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#CATALOG,B-267'
		
		# there is a single non-auction 'Private Contract Sale' event, and it is referred to by the catalog text
		pvt_sale = activities['tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#PRIVATE_CONTRACT_SALE-EVENT,CATALOGNUMBER,B-267']
		self.assertEqual(pvt_sale['_label'], 'Private Contract Sale Event for B-267')
		self.assertIn(expected_catalog_text_id, {r.get('id') for r in pvt_sale['referred_to_by']})
		
		# there are 3 physical Exhibition Catalogs
		phys_catalogs = [o for o in objects.values() if o['classified_as'][0]['_label'] == 'Exhibition Catalog']
		self.assertEqual(len(phys_catalogs), 3)
		
		# all 3 physical catalogs carry the same catalog text
		catalog_text_ids = set()
		for o in phys_catalogs:
			for text in o['carries']:
				catalog_text_ids.add(text['id'])
		self.assertEqual(catalog_text_ids, {expected_catalog_text_id})
		self.assertIn(expected_catalog_text_id, texts)

		catalog_text = texts[expected_catalog_text_id]
		
		self.assertEqual(len(objects), 7) # 3 physical catalogs and 4 objects sold

	def verify_sales(self, output):
		objects = output['model-object']
# 		auctions = output['model-auction-of-lot']
		activities = output['model-activity']
		texts = output['model-lo']

# 		event_key = 'tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#AUCTION,B-267,LOT,0001,DATE,1817'
# 		event = auctions[event_key]
# 		self.assertEqual({c['_label'] for c in event['classified_as']}, {})
# 		pprint.pprint(list(auctions.keys()))
# 		pprint.pprint(event)
# 		expected_catalog_text_id = 'tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#CATALOG,B-267'
# 		
# 		# there is a single non-auction 'Private Contract Sale' event, and it is referred to by the catalog text
# 		pvt_sale = activities['tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#PRIVATE_CONTRACT_SALE-EVENT,CATALOGNUMBER,B-267']
# 		self.assertEqual(pvt_sale['_label'], 'Private Contract Sale Event for B-267')
# 		self.assertIn(expected_catalog_text_id, {r['id'] for r in pvt_sale['referred_to_by']})
# 		
# 		# there are 3 physical Exhibition Catalogs
# 		phys_catalogs = [o for o in objects.values() if o['classified_as'][0]['_label'] == 'Exhibition Catalog']
# 		self.assertEqual(len(phys_catalogs), 3)
# 		
# 		# all 3 physical catalogs carry the same catalog text
# 		catalog_text_ids = set()
# 		for o in phys_catalogs:
# 			for text in o['carries']:
# 				catalog_text_ids.add(text['id'])
# 		self.assertEqual(catalog_text_ids, {expected_catalog_text_id})
# 		self.assertIn(expected_catalog_text_id, texts)
# 
# 		catalog_text = texts[expected_catalog_text_id]
# 		
# 		self.assertEqual(len(objects), 7) # 3 physical catalogs and 4 objects sold


if __name__ == '__main__':
	unittest.main()
