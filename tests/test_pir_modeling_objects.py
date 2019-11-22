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

class PIRModelingTest_Objects(TestProvenancePipelineOutput):
	def test_modeling_for_objects(self):
		'''
		Test that all HumanMadeObjects are linked to a corresponding VisualItem.
		'''
		output = self.run_pipeline('objects')

		# there is one object and one visual item
		objects = output['model-object']
		visitems = output['model-visual-item']
		self.assertEqual(len(objects), 1)
		self.assertEqual(len(visitems), 1)

		object = next(iter(objects.values()))
		vi = next(iter(visitems.values()))
		
		# the object shows the visual item
		self.assertEqual(len(object['shows']), 1)
		self.assertEqual(object['shows'][0]['type'], 'VisualItem')
		shows_id = object['shows'][0]['id']
		self.assertEqual(shows_id, vi['id'])
		
		# The visual item's label contains the title of the object
		object_title = [n['content'] for n in object.get('identified_by', []) if n['classified_as'][0]['_label'] == 'Primary Name'][0]
		vi_label = vi['_label']
		self.assertIn(object_title, vi_label)


if __name__ == '__main__':
	unittest.main()
