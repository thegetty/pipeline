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

from tests import TestAATAPipelineOutput
from cromulent import vocab

vocab.add_attribute_assignment_check()

class AATAModelingTest_Corp(TestAATAPipelineOutput):
	'''
	Tests the modeling of a corporate body with expected name and location data.
	'''
	
	def test_modeling_corp(self):
		output = self.run_pipeline('corp')
		self.verify_corps(output)
	
	def verify_corps(self, output):
		groups = output['model-groups']
		places = output['model-place']
		
		# 2 groups: the test data, and GCI as the assignor of an ID
		# 3 places: the residence of the test group, and two enclosing `part_of` places
		self.assertEqual(len(groups), 2)
		self.assertEqual(len(places), 3)
		
		base_uri = 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:aata#'
		expected_gaia_ids = ['3032']
		expected_uris = [f'{base_uri}Corp,{id}' for id in expected_gaia_ids]
		for uri in expected_uris:
			self.assertIn(uri, groups)
		
		g3032 = groups[expected_uris[0]]
		self.assertEqual(g3032['type'], 'Group')
		self.verify_content(g3032, identified_by='American School of Classical Studies at Athens')
		g3032_geog = g3032['residence']
		self.assertEqual(len(g3032_geog), 1)
		g3032_place_uri = g3032_geog[0]['id']
		g3032_place = places[g3032_place_uri]
		self.verify_place_hierarchy(places, g3032_place, ['Princeton', 'New Jersey', 'United States'])

if __name__ == '__main__':
	unittest.main()
