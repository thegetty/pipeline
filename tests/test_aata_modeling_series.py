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

class AATAModelingTest_Series(TestAATAPipelineOutput):
	'''
	This tests the modeling of a single journal with one issue.
	* The issue should be part_of the journal.
	* Both should have various identifiers.
	* Both should be used by a publishing activity which has a timespan.
	'''

	def test_modeling_journal(self):
		output = self.run_pipeline('series')
		self.verify_journal(output)
	
	def verify_journal(self, output):
		texts = output['model-lo']
		groups = output['model-groups']
		
		# one series
		self.assertEqual(len(texts), 1)
		
		# GCI as the assignor of identifiers
		self.assertEqual(len(groups), 1)
		
		base_uri = 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:aata#'
		suri = f'{base_uri}Series,4'
		expected_uris = [suri]
		
		for uri in expected_uris:
			self.assertIn(uri, texts)
		
		s = texts[expected_uris[0]]
		self.assertEqual(s['type'], 'LinguisticObject')
		self.verify_property(s, '_label', classified_as='Series')
		s_identifiers = self.get_typed_identifiers(s)
		self.assertEqual(s_identifiers['Primary Name'], 'History of science and technology in India')
		self.assertEqual(s_identifiers['Owner-Assigned Number'], '4')
		self.assertEqual(s_identifiers['Title'], 'History of science and technology in India')

		s_publishings = s['used_for']
		self.assertEqual(len(s_publishings), 1)
		s_publishing = s_publishings[0]
		self.assertEqual(set(self.get_classification_labels(s_publishing)), {'Publishing'})
		self.assertEqual(s_publishing['timespan']['begin_of_the_begin'], '1969-01-01:00:00:00Z')
		self.assertEqual(s_publishing['timespan']['end_of_the_end'], '1994-01-01:00:00:00Z')


if __name__ == '__main__':
	unittest.main()
