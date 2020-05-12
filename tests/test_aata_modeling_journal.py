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

class AATAModelingTest_Journal(TestAATAPipelineOutput):
	'''
	This tests the modeling of a single journal with one issue.
	* The issue should be part_of the journal.
	* Both should have various identifiers.
	* Both should be used by a publishing activity which has a timespan.
	'''

	def test_modeling_journal(self):
		output = self.run_pipeline('journal')
		self.verify_journal(output)
	
	def verify_journal(self, output):
		texts = output['model-lo']
		groups = output['model-groups']
		
		# one journal, one issue
		self.assertEqual(len(texts), 2)
		
		# GCI as the assignor of identifiers
		self.assertEqual(len(groups), 1)
		
		base_uri = 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:aata#'
		juri = f'{base_uri}Journal,2'
		iuri = f'{base_uri}Journal,2,Issue,38548'
		expected_uris = [juri, iuri]
		
		for uri in expected_uris:
			self.assertIn(uri, texts)
		
		j = texts[expected_uris[0]]
		self.assertEqual(j['type'], 'LinguisticObject')
		self.verify_property(j, '_label', classified_as='Journal')
		j_identifiers = self.get_typed_identifiers(j)
		self.assertEqual(j_identifiers['Primary Name'], 'Green chemistry')
		self.assertEqual(j_identifiers['ISSN Identifier'], '1463-9262')
		self.assertEqual(j_identifiers['Owner-Assigned Number'], '2')
		self.assertEqual(j_identifiers['Title'], {
			'Green chemistry',
			'Green chemistry: an international journal and green '
			'chemistry resource',
			'Green chemistry: cutting-edge research for a greener '
			'sustainable future'
		})

		self.verify_property(j, 'content', referred_to_by='OCLC 40966731')
		self.verify_property(j, 'content', referred_to_by='12/year')

		i = texts[expected_uris[1]]
		self.assertEqual(i['type'], 'LinguisticObject')
		self.verify_property(i, '_label', classified_as='Issue')
		
		j_publishings = j['used_for']
		self.assertEqual(len(j_publishings), 1)
		j_publishing = j_publishings[0]
		self.assertEqual(set(self.get_classification_labels(j_publishing)), {'Publishing'})
		self.assertEqual(j_publishing['timespan']['begin_of_the_begin'], '1999-01-01:00:00:00Z')
		
		issue_part_of = {p['id'] for p in i['part_of']}
		self.assertIn(juri, issue_part_of)

		i_identifiers = self.get_typed_identifiers(i)
		self.assertEqual(i_identifiers['Title'], 'Green chemistry (v. 9, n. 9)')
		self.assertEqual(i_identifiers['Owner-Assigned Number'], '38548')

		i_publishings = i['used_for']
		self.assertEqual(len(i_publishings), 1)
		i_publishing = i_publishings[0]
		self.assertEqual(set(self.get_classification_labels(i_publishing)), {'Publishing'})
		self.assertEqual(i_publishing['timespan']['begin_of_the_begin'], '2007-01-01:00:00:00Z')
		self.assertEqual(i_publishing['timespan']['end_of_the_end'], '2008-01-01:00:00:00Z')
		self.verify_property(i_publishing['timespan'], 'content', identified_by='2007')


if __name__ == '__main__':
	unittest.main()
