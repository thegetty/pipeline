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

class PIRModelingTest_Destruction(TestSalesPipelineOutput):
	def test_modeling_for_destruction(self):
		'''
		All objects in this set have a 'destroyed_by' property that has type 'Destruction'.
		
		This destruction information comes from either the `lot_notes` or the
		`present_loc_geog` fields.
		'''
		output = self.run_pipeline('destruction')

		objects = output['model-object']
		self.assertEqual(len(objects), 15)
		for o in objects.values():
			if 'destroyed_by' not in o:
				print(f"****** BAD RECORD: {o['id']}")
			self.assertIn('destroyed_by', o)
			self.assertEqual(o['destroyed_by']['type'], 'Destruction')


if __name__ == '__main__':
	unittest.main()
