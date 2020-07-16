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

class PIRModelingTest_Catalog(TestSalesPipelineOutput):
	def test_modeling_for_catalogs(self):
		'''
		Test modeling of both linguistic auction catalogs, as well as known physical
		copies of those catalogs.
		'''
		output = self.run_pipeline('catalogs')
		objects = output['model-object']
		sale_activities = output['model-sale-activity']
		los = output['model-lo']
		self.assertEqual(len(objects), 2)
		self.assertEqual(len(sale_activities), 1)
		self.assertEqual(len(los), 1)

		# physical catalog copies carry the same catalog linguistic object
		catalogs = {o['carries'][0]['id'] for o in objects.values()}
		lingobj_ids = set([l['id'] for l in los.values()])
		self.assertEqual(catalogs, lingobj_ids)

		# the auction event is referred to by the catalog linguistic object
		event_subjects = {s['id'] for e in sale_activities.values() for s in e['referred_to_by'] if 'id' in s}
		self.assertEqual(catalogs, event_subjects)


if __name__ == '__main__':
	unittest.main()
