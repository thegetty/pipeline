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

class PIRModelingTest_OrAnonymousModifiers(TestProvenancePipelineOutput):
	def test_modeling_for_or_anonymous_artist(self):
		'''
		This object record has one named artist, and another anonymous artist, modified
		with "or". "Or anonymous" records are modeled as uncertainty about the named
		artist (using an attribute assignment classified as 'Possibly').
		'''
		output = self.run_pipeline('or_anon')

		objects = output['model-object']
		people = output['model-person']
		
		or_anon_obj = objects['tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#OBJ,Br-A559,0103,1758-05-24']
		production = or_anon_obj['produced_by']
		attr_assignment = production['attributed_by'][0]
		self.assertEqual(attr_assignment['assigned_property'], 'part')
		self.assertEqual(attr_assignment['classified_as'][0]['_label'], 'Possibly')
		uncertain_production = attr_assignment['assigned']
		self.assertEqual(uncertain_production['type'], 'Production')
		people = {p['_label'] for p in uncertain_production['carried_out_by']}
		self.assertEqual(people, {'TILLEMANS, PETER'})


if __name__ == '__main__':
	unittest.main()
