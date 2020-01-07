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

from tests import TestProvenancePipelineOutput
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_Withdrawn(TestProvenancePipelineOutput):
	def test_modeling_for_withdrawn_records(self):
		'''
		An object with transaction type "withdrawn" should be modeled, but not belong to
		any auction of lot set of objects.
		
		In this case, there is a lot of two objects, only one of which is withdrawn.
		We expect the Auction of Lot set to only contain one object, but the text record
		of the auction to reference two objects.
		'''
		output = self.run_pipeline('withdrawn')
		objects = output['model-object']
		sets = output['model-set']
		auctions = output['model-auction-of-lot']
		texts = output['model-lo']
		
		self.assertEqual(len(objects), 2)
		self.assertEqual(len(texts), 2)
		self.assertEqual(len(auctions), 1)

		withdrawn_obj = objects['tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#OBJECT,B-447,0024a,1827-05-22']
		withdrawn_rec = texts['tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#CATALOG,B-447,RECORD,75734']

		sold_obj = objects['tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#OBJECT,B-447,24,1827-05-22']
		sold_rec = texts['tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#CATALOG,B-447,RECORD,75735']
		
		# sold object is a member of the auction set...
		self.assertEqual({o['id'] for o in sold_obj['member_of']}, {'tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#AUCTION,B-447,LOT,24,DATE,1827-05-22-Set'})
		# ... but the withdrawn object isn't
		self.assertNotIn('member_of', withdrawn_obj)
		
		# the withdrawn record is about the withdrawn object
		withdrawn_rec_about = {o['id'] for o in withdrawn_rec['about']}
		self.assertEqual(withdrawn_rec_about, {withdrawn_obj['id']})


if __name__ == '__main__':
	unittest.main()
