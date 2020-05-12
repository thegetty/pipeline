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

from pipeline.util import CaseFoldingSet
from tests import TestAATAPipelineOutput
from cromulent import vocab

vocab.add_attribute_assignment_check()

class AATAModelingTest_Places(TestAATAPipelineOutput):
	'''
	Test the modeling and classification of four different place/types:
	
	* A Nation (Burundi)
	* A State (California, being `part_of` the Nation `United States`)
	* An Island Group (Caribbean Islands)
	* A Group of Nations (European Union)
	'''

	def test_modeling_geog(self):
		output = self.run_pipeline('geog')
		self.verify_geog(output)
	
	def verify_geog(self, output):
		places = output['model-place']
		
		# 4 modeled places, and one enclosing country
		self.assertEqual(len(places), 5)
		
		base_uri = 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:aata#'
		expected_ids = ['Named,Burundi', 'Named,United%20States,California', 'Named,United%20States', 'ID,130791', 'ID,132543']
		expected_uris = [f'{base_uri}Place,{id}' for id in expected_ids]
		for uri in expected_uris:
			self.assertIn(uri, places)
		
		for p in places.values():
			self.assertEqual(p['type'], 'Place')
			
		burundi = places[expected_uris[0]]
		california = places[expected_uris[1]]
		us = places[expected_uris[2]]
		islands = places[expected_uris[3]]
		eu = places[expected_uris[4]]
		
		self.verify_content(burundi, identified_by='Burundi')
		self.assertIn('nation', CaseFoldingSet(self.get_classification_labels(burundi)))
		
		self.verify_content(california, identified_by='California')
		self.assertIn('state', CaseFoldingSet(self.get_classification_labels(california)))
		self.assertEqual(len(california.get('part_of', [])), 1)
		self.assertEqual(california['part_of'][0]['id'], us['id'])

		self.verify_content(us, identified_by='United States')
		self.assertIn('nation', CaseFoldingSet(self.get_classification_labels(us)))

		self.verify_content(islands, identified_by='Caribbean Islands')
		self.assertIn('island group', CaseFoldingSet(self.get_classification_labels(islands)))

		self.verify_content(eu, identified_by='European Union')
		self.assertIn('group of nations/states/cities', CaseFoldingSet(self.get_classification_labels(eu)))


if __name__ == '__main__':
	unittest.main()
