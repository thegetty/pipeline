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

class PIRModelingTest_MultiArtist(TestProvenancePipelineOutput):
	def test_modeling_for_multi_artists(self):
		'''
		The object in this set has a production event that has 3 sub-events, pointing to
		the three artists modeled as people. The seller is also modeled in an assumed
		Auction of Lot (assumed because the transaction type is "unknown").
		'''
		output = self.run_pipeline('multiartist')

		objects = output['model-object']
		people = output['model-person']
		self.assertEqual(len(objects), 1)
		self.assertEqual(len(people), 4) # there are 3 artists and 1 seller
		people_ids = set([p['id'] for p in people.values()])
		
		seller_id = {'tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#PERSON,AUTH,Wit%2C%20Jacomo%20de'}
		artist_people_ids = people_ids - seller_id
		
		object = next(iter(objects.values()))
		event = object['produced_by']
		production_artists = [e['carried_out_by'][0] for e in event['part']]
		production_artist_ids = set([a['id'] for a in production_artists])
		self.assertEqual(artist_people_ids, production_artist_ids)


if __name__ == '__main__':
	unittest.main()
