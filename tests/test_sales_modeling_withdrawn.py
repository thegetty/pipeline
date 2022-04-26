#!/usr/bin/env python3 -B
import unittest
import os
import os.path
import hashlib
import json
import uuid
import pprint
import inspect
from pathlib import Path
import warnings

from tests import TestSalesPipelineOutput
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_Withdrawn(TestSalesPipelineOutput):
	def test_modeling_for_withdrawn_records(self):
		'''
		An object with transaction type "withdrawn" should be modeled, and belong to the
		auction of lot set of objects. It should also appear in a previous transaction
		that led to the current seller acquiring it.
		
		In this case, there is a lot of two objects, only one of which is withdrawn.
		We expect the Auction of Lot set to contain two objects, the text record
		of the auction to reference two objects, and the procurement to have only one
		acquisition.
		'''
		output = self.run_pipeline('withdrawn')
		objects = output['model-object']
		sets = output['model-set']
		auctions = output['model-sale-activity']
		texts = output['model-lo']
		activities = output['model-activity']
		procurements = activities.values()
		
		self.assertEqual(len(objects), 2)
		self.assertEqual(len(texts), 3)
		self.assertEqual(len(auctions), 1)
		self.assertEqual(len(procurements), 3)
		
		procurement_labels = {p['_label'] for p in procurements}
		self.assertEqual(procurement_labels, {
			'Event leading to Ownership of Br-3039 0082[a] (1827-11-24)',
			'Event leading to Ownership of Br-3039 0082[b] (1827-11-24)',
	 		'Sale of Br-3039 0082 (1827-11-24)'
	 	})
		
		procurement = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PROV,Br-3039,1827-11-24,0082']
		parts = procurement.get('part', [])
		self.assertEqual(len(parts), 6)
		part_types = {p['type'] for p in parts}
		self.assertEqual(part_types, {'Acquisition', 'AttributeAssignment', 'Payment', 'TransferOfCustody'})
		acqs = [p for p in parts if p['type'] == 'Acquisition']
		self.assertEqual(len(acqs), 1)

		withdrawn_obj = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-3039,0082%5Bb%5D,1827-11-24']
		withdrawn_rec = texts['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,Br-3039,RECORD,346889']

		sold_obj = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-3039,0082%5Ba%5D,1827-11-24']
		sold_rec = texts['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,Br-3039,RECORD,346890']
		
		# both sold and withdrawn objects are members of the auction set...
		set_id = 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION,Br-3039,0082,1827-11-24-Set'
		self.assertEqual({o['id'] for o in sold_obj['member_of']}, {set_id})
		self.assertEqual({o['id'] for o in withdrawn_obj['member_of']}, {set_id})

		# the withdrawn record is about the withdrawn object
		withdrawn_rec_about = {o['id'] for o in withdrawn_rec['about']}
		self.assertIn(withdrawn_obj['id'], withdrawn_rec_about)


if __name__ == '__main__':
	unittest.main()
