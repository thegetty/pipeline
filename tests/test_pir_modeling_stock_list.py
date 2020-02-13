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

class PIRModelingTest_StockListSales(TestProvenancePipelineOutput):
	def test_modeling_stock_list_sales(self):
		'''
		Test for modeling of Stock List Sales.
		'''
		output = self.run_pipeline('stock_list')
		self.verify_catalogs(output)
		self.verify_sales(output)
	
	def verify_catalogs(self, output):
		'''
		For this non-auction sale event, there should be a 'Stock List' event,
		and all physical copies of the sales catalog should be both classified as an
		'Exhibition Catalog', and carry the same text.
		'''
		objects = output['model-object']
		activities = output['model-activity']
		texts = output['model-lo']

		expected_catalog_text_id = 'tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#CATALOG,Br-541'
		expected_event_id = 'tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#STOCK_LIST-EVENT,CATALOGNUMBER,Br-541'
		
		# there is a single non-auction 'Private Contract Sale' event, and it is referred to by the catalog text
		pvt_sale = activities[expected_event_id]
		self.assertEqual(pvt_sale['_label'], 'Stock List Event for Br-541')
		self.assertIn(expected_catalog_text_id, {r.get('id') for r in pvt_sale['referred_to_by']})
		
		# there is 1 physical Accession Catalog
		phys_catalogs = [o for o in objects.values() if o['classified_as'][0]['_label'] == 'Accession Catalog']
		self.assertEqual(len(phys_catalogs), 1)
		
		# all physical catalogs carry the same catalog text
		catalog_text_ids = set()
		for o in phys_catalogs:
			for text in o['carries']:
				catalog_text_ids.add(text['id'])
		self.assertEqual(catalog_text_ids, {expected_catalog_text_id})
		self.assertIn(expected_catalog_text_id, texts)

		catalog_text = texts[expected_catalog_text_id]
		
		self.assertEqual(len(objects), 2) # 1 physical catalog and 1 object sold

	def verify_sales(self, output):
		'''
		For a private contract sale record, there should be:
		
		* A private sale activity classified as an Exhibition
		* An Object Set classified as a Collection
		* A HumanMadeObject classified as a Painting, and belonging to the Object Set
		* A Provenance Entry for the private sale
		'''
		objects = output['model-object']
		activities = output['model-activity']
		sets = output['model-set']
		texts = output['model-lo']

		hmo_key = 'tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#OBJECT,Br-541,%5B0001%5D,1808'
		hmo = objects[hmo_key]
		
		prov_entry_curr = activities['tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#AUCTION,Br-541,LOT,%5B0001%5D,DATE,1808']
		
		event_key = 'tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#STOCK_LIST-EVENT,CATALOGNUMBER,Br-541'
		sale_event = activities[event_key]
		
		object_set_key = 'tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#AUCTION,Br-541,LOT,%5B0001%5D,DATE,1808-Set'
		object_set = sets[object_set_key]
		
		self.assertEqual({c['_label'] for c in sale_event['classified_as']}, {'Exhibiting'})
		self.assertEqual({c['_label'] for c in object_set['classified_as']}, {'Collection'})

		self.assertIn(object_set_key, {s['id'] for s in hmo['member_of']})
		
		# There are no acquisitions or payments as the transaction is 'unknown'.
		self.assertNotIn('part', prov_entry_curr)


if __name__ == '__main__':
	unittest.main()
