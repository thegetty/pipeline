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

class PIRModelingTest_PrivateContractSales(TestProvenancePipelineOutput):
	def test_modeling_private_contract_sales(self):
		'''
		Test for modeling of Private Contract Sales.
		'''
		output = self.run_pipeline('private_contract_sales')
		self.verify_catalogs(output)
		self.verify_sales(output)
	
	def verify_catalogs(self, output):
		'''
		For this non-auction sale event, there should be a 'Private Contract Sale' event,
		and all physical copies of the sales catalog should be both classified as an
		'Exhibition Catalog', and carry the same text.
		'''
		objects = output['model-object']
		activities = output['model-activity']
		texts = output['model-lo']

		expected_catalog_text_id = 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:provenance#CATALOG,B-267'
		
		# there is a single non-auction 'Private Contract Sale' event, and it is referred to by the catalog text
		pvt_sale = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:provenance#PRIVATE_CONTRACT_SALE-EVENT,B-267']
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
		'''
		For a private contract sale record, there should be:
		
		* A private sale activity classified as an Exhibition
		* An Object Set classified as a Collection, having an Asking Price
		* A HumanMadeObject classified as a Painting, and belonging to the Object Set
		* A Provenance Entry for the private sale, including:
			* An Acquisition with transfer of title from the Seller to the Buyer
			* A Transfer of Custody from the Seller to the Sale Organizer
			* A Transfer of Custody from the Sale Organizer to the Buyer
		* A Provenance Entry for the seller's previous ownership, including:
			* An Acquisition with transfer of title to the Seller
			* A Transfer of Custody to the Seller
		'''
		objects = output['model-object']
		activities = output['model-activity']
		sets = output['model-set']
		texts = output['model-lo']

		hmo_key = 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:provenance#OBJ,B-267,0001,1817'
		hmo = objects[hmo_key]
		
		prov_entry_curr = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:provenance#PROV,B-267,1817,0001']
		prov_entry_prev = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:provenance#OBJ,B-267,0001,1817-seller-0-ProvenanceEntry']
		
		event_key = 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:provenance#PRIVATE_CONTRACT_SALE-EVENT,B-267'
		sale_event = activities[event_key]
		
		object_set_key = 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:provenance#AUCTION,B-267,0001,1817-Set'
		object_set = sets[object_set_key]
		
		self.assertEqual({c['_label'] for c in sale_event['classified_as']}, {'Exhibiting'})
		self.assertEqual({c['_label'] for c in object_set['classified_as']}, {'Collection'})

		prices = [d for d in object_set['dimension'] if d['type'] == 'MonetaryAmount']
		self.assertEqual(len(prices), 1)
		self.assertEqual({c['_label'] for c in prices[0]['classified_as']}, {'Asking Price'})
		self.assertEqual(prices[0]['_label'], "50,000 frs")
		self.assertEqual(prices[0]['currency']['_label'], 'French Francs')
		self.assertEqual(prices[0]['value'], 50000)
		self.assertEqual({c['_label'] for c in object_set['dimension']}, {"50,000 frs"})

		self.assertIn(object_set_key, {s['id'] for s in hmo['member_of']})
		
		self.assertEqual(sorted([p['type'] for p in prov_entry_curr['part']]), ['Acquisition', 'Payment', 'TransferOfCustody', 'TransferOfCustody'])
		acq = [p for p in prov_entry_curr['part'] if p['type'] == 'Acquisition'][0]
		xfers = [p for p in prov_entry_curr['part'] if p['type'] == 'TransferOfCustody']

		# seller   : Havre, Jean-Michel-Antoine-Joseph-Louis, baron van		500439105
		# buyer    : Stier d'Aertselaer, Henri-Joseph, baron				500440144
		# organizer: Anonymous												22949
		event_organizer = 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:provenance#HOUSE,PI,22949,0' # TODO this should be pulled from the data

		buyers = acq['transferred_title_to']
		self.assertEqual(len(buyers), 1)
		buyer = buyers[0]['id']

		sellers = acq['transferred_title_from']
		self.assertEqual(len(sellers), 1)
		seller = sellers[0]['id']
		
		acq_objects = acq['transferred_title_of']
		self.assertEqual(len(acq_objects), 1)
		self.assertEqual(acq_objects[0]['id'], hmo['id'])
		
		for xfer in xfers:
			xfer_objects = xfer['transferred_custody_of']
			self.assertEqual(len(xfer_objects), 1)
			self.assertEqual(xfer_objects[0]['id'], hmo['id'])

		xfer_pairs = {(xfer['transferred_custody_from'][0]['id'], xfer['transferred_custody_to'][0]['id']) for xfer in xfers}
		expected_pairs = {(seller, event_organizer), (event_organizer, buyer)}
		self.assertEqual(xfer_pairs, expected_pairs)

		self.assertEqual(sorted([p['type'] for p in prov_entry_prev['part']]), ['Acquisition', 'TransferOfCustody'])
		prev_acq = [p for p in prov_entry_prev['part'] if p['type'] == 'Acquisition'][0]
		prev_xfers = [p for p in prov_entry_prev['part'] if p['type'] == 'TransferOfCustody']

		prev_buyers = prev_acq['transferred_title_to']
		self.assertEqual(len(prev_buyers), 1)
		prev_buyer = prev_buyers[0]['id']

		self.assertEqual(prev_buyer, seller)
		self.assertEqual(len(prev_xfers), 1)
		for xfer in prev_xfers:
			xfer_objects = xfer['transferred_custody_of']
			self.assertEqual(len(xfer_objects), 1)
			self.assertEqual(xfer_objects[0]['id'], hmo['id'])
		prev_xfer = prev_xfers[0]
		self.assertEqual({p['id'] for p in prev_xfer['transferred_custody_to']}, {seller})


if __name__ == '__main__':
	unittest.main()
