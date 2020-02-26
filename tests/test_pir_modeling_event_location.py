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
from contextlib import suppress

from tests import TestSalesPipelineOutput
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_EventLocation(TestSalesPipelineOutput):
	def test_modeling_for_event_locations(self):
		'''
		Test for modeling of the locations of auction events, including a Place hierarchy.
		'''
		output = self.run_pipeline('event_location')

		events = output['model-activity']
		places = output['model-place']
		self.assertEqual(len(events), 1)
		self.assertEqual(len(places), 3)
		
		# The auction event took place at a location which is in a hierarchy of 3 places
		# (address, city, country)
		event = next(iter(events.values()))
		event_location = event['took_place_at'][0]
		place = places[event_location['id']]

		names = []
		while place:
			name = place['_label']
			names.append(name)
			parent = place.get('part_of', [None])[0]
			if parent:
				parent_id = parent['id']
				place = places.get(parent_id, None)
			else:
				place = None
		self.assertEqual(names, ['Kaiserstr. 187, Karlsruhe, Germany', 'Karlsruhe, Germany', 'Germany'])


if __name__ == '__main__':
	unittest.main()
