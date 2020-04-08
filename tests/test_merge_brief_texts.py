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

class TestMergedBriefTexts(unittest.TestCase):
	def setUp(self):
		pass

	def tearDown(self):
		pass

	def person(self, label):
		p = model.Person(ident='http://example.org/person', label=label)
		p.referred_to_by = vocab.Note(ident='', content='This is Eve')
		return p

	def test_merge(self):
		p1 = self.person('Eve 1')
		p2 = self.person('Eve 2')

		merger = CromObjectMerger()
		merger.merge(p1, p2)

		referrers = p1.referred_to_by
		self.assertEqual(len(referrers), 1)

if __name__ == '__main__':
	unittest.main()
