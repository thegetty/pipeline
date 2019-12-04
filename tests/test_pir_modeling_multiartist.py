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

from pipeline.projects.provenance.util import pir_uri
from tests import TestProvenancePipelineOutput
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_MultiArtist(TestProvenancePipelineOutput):
	def test_modeling_for_multi_artists(self):
		'''
		The object in this set has a production event that has 3 sub-events, pointing to
		the three artists modeled as people. No other people are modeled in the dataset.
		'''
		output = self.run_pipeline('multiartist')

		objects = output['model-object']
		people = output['model-person']
		self.assertEqual(len(objects), 1)
		self.assertEqual(len(people), 3)
		people_ids = set([p['id'] for p in people.values()])
		object = next(iter(objects.values()))
		event = object['produced_by']
		artists = [e['carried_out_by'][0] for e in event['part']]
		artist_ids = set([a['id'] for a in artists])
		self.assertEqual(people_ids, artist_ids)


if __name__ == '__main__':
	unittest.main()
