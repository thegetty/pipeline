#!/usr/bin/env python3 -B
import unittest
import os
import os.path
import hashlib
import json
import uuid
import pprint

from cromulent import model, vocab
from cromulent.model import factory
from pipeline.util import CromObjectMerger

class TestMergedDimensions(unittest.TestCase):
	def setUp(self):
		pass

	def tearDown(self):
		pass

	def test_pipeline_pir(self):
		'''
		When dimensions get merged, the Unknown physical dimension classification (300055642)
		gets dropped if there are any other classifications.
		'''
		h1 = vocab.Height(ident='', content=9.0)
		h1.unit = vocab.instances.get('inches')
		self.assertEqual({c._label for c in h1.classified_as}, {'Height'})

		h2 = vocab.PhysicalDimension(ident='', content=9.0)
		self.assertEqual({c._label for c in h2.classified_as}, {'Unknown physical dimension'})
		
		merger = CromObjectMerger()
		
		h = merger.merge(h1, h2)
		self.assertEqual({c._label for c in h.classified_as}, {'Height'})
		

if __name__ == '__main__':
	unittest.main()
