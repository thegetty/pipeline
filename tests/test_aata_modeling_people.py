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

class AATAModelingTest_People(TestAATAPipelineOutput):
	'''
	Tests the modeling of two people with expected name data.
	'''
	
	def test_modeling_person(self):
		output = self.run_pipeline('person')
		self.verify_people(output)
	
	def verify_people(self, output):
		'''
		For this non-auction sale event, there should be a 'Private Contract Sale' event,
		and all physical copies of the sales catalog should be both classified as an
		'Exhibition Catalog', and carry the same text.
		'''
		people = output['model-person']
		self.assertEqual(len(people), 2)
		
		base_uri = 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:aata#'
		expected_gaia_ids = ['17245', '17246']
		expected_uris = [f'{base_uri}Person,GAIA,{id}' for id in expected_gaia_ids]
		for uri in expected_uris:
			self.assertIn(uri, people)
		
		p17245 = people[expected_uris[0]]
		self.assertEqual(p17245['type'], 'Person')
		self.verify_content(p17245, identified_by='O.P. Agrawal')
		
		p17246 = people[expected_uris[1]]
		self.assertEqual(p17246['type'], 'Person')
		self.verify_content(p17246, identified_by='Giuseppe Albanese')


if __name__ == '__main__':
	unittest.main()
