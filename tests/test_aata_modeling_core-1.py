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

class AATAModelingTest_Core(TestAATAPipelineOutput):
	'''
	Tests the modeling of a core abstract record for a monograph.
	
	* Identifiers include a primary name, title, translated title, and system- and owner-assigned IDs
	* Includes indexing classifications terms
	* Used by a publishing activity which has an associated location
	* Created by a creation activity which has a part carried out by the author
	* Is referred_to_by an Abstract as well as a Description and Pagination Statement
	'''

	def test_modeling_core(self):
		output = self.run_pipeline('core-1')
		self.verify_core(output)
	
	def verify_core(self, output):
		texts = output['model-lo']
		places = output['model-place']
		people = output['model-person']
		self.assertEqual(len(texts), 1)
		
		base_uri = 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:aata#'
		expected_gaia_ids = ['1']
		expected_uris = [f'{base_uri}Article,{id}' for id in expected_gaia_ids]
		for uri in expected_uris:
			self.assertIn(uri, texts)
		
		a1 = texts[expected_uris[0]]
		self.assertEqual(a1['type'], 'LinguisticObject')
		a_identifiers = self.get_typed_identifiers(a1)
		self.assertEqual(a_identifiers['Primary Name'], 'Méthodes modernes: description des méthodes modernes de restauration des peintures sur toile')
		self.assertEqual(a_identifiers['Title'], 'Méthodes modernes: description des méthodes modernes de restauration des peintures sur toile')
		self.assertEqual(a_identifiers['Translated Title'], 'Modern methods: description of modern methods of restoration of canvas paintings')
		self.assertEqual(a_identifiers['System-Assigned Number'], '1')
		for n in ('27-546', '1990-1'):
			self.assertIn(n, a_identifiers['Owner-Assigned Number'])
		
		self.assertIn('Monograph', set(self.get_classification_labels(a1)))
		self.assertIn('Pigments, paints, and paintings', set(self.get_classification_labels(a1)))

		expected_about = {
			'history of conservation',
			'lining (process)',
			'textbooks',
			'paintings (visual works)'
		}
		got_about = set(a['_label'] for a in a1['about'])
		self.assertEqual(got_about, expected_about)

		creation = a1['created_by']
		self.assertEqual(creation['type'], 'Creation')
		creation_parts = creation['part']
		creators = [p for part in creation_parts for p in part['carried_out_by']]
		names = {c['_label'] for c in creators}
		self.assertEqual(names, {'Petéus, Thomas'})

		a_referrers = self.get_typed_referrers(a1)
		self.assertTrue(a_referrers.get('Abstract', '').startswith('Translation of a textbook for students at the Danish Royal Institute of Restoration'))
		self.assertEqual(a_referrers['Description'], '44 ills., product table, bibliog.')
		self.assertEqual(a_referrers['Pagination Statement'], '63 p.')
		
		a_publishings = a1['used_for']
		self.assertEqual(len(a_publishings), 1)
		a_publishing = a_publishings[0]
		self.assertEqual(set(self.get_classification_labels(a_publishing)), {'Publishing'})
		pub_locations = a_publishing['took_place_at']
		self.assertEqual(len(pub_locations), 1)
		pub_location = pub_locations[0]
		self.assertIn(pub_location['id'], places)
		pub_place = places[pub_location['id']]
		self.assertEqual(pub_place['_label'], 'Gothenburg')


if __name__ == '__main__':
	unittest.main()
