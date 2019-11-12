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

from tests import TestProvenancePipelineOutput

class PIRModelingTest_MultiLot(TestProvenancePipelineOutput):
	def test_modeling_for_multi_lot_procurements(self):
		'''
		Test for modeling of single procurements for which a single payment was made
		for multiple auction lots.
		'''
		output = self.run_pipeline('multilot')

		objects = output['model-object']
		activities = list(output['model-activity'].values())
		self.assertEqual(len(activities), 3)
		
		by_type = {k: list(v) for k,v in groupby(activities, key=lambda a: a['classified_as'][0]['_label'])}
		self.assertEqual(sorted(by_type.keys()), ['Auction of Lot', 'Procurement'])
		
		# there are 2 lots and 1 procurement
		self.assertEqual(len(by_type['Auction of Lot']), 2)
		self.assertEqual(len(by_type['Procurement']), 1)

		lots = by_type['Auction of Lot']
		procurement = by_type['Procurement'][0]

		# procurement has 1 payment and 2 acquisitions
		parts = procurement['part']
		proc_types = sorted([a['type'] for a in parts])
		self.assertEqual(proc_types, ['Acquisition', 'Acquisition', 'Payment'])
		acqs = [a for a in parts if a['type'] == 'Acquisition']
		paym = [a for a in parts if a['type'] == 'Payment'][0]

		# the objects in the 2 acquisitions are the same as the objects in the 2 lots
		objects_from_acqs = set()
		for acq in acqs:
			for obj in acq.get('transferred_title_of', []):
				objects_from_acqs.add(obj['id'])
		
		object_sets_for_lots = set([s['id'] for l in lots for s in l.get('used_specific_object', [])])

		objects_from_lots = set()
		for obj in objects.values():
			for m in obj.get('member_of', []):
				if m['id'] in object_sets_for_lots:
					objects_from_lots.add(obj['id'])

		self.assertEqual(objects_from_acqs, objects_from_lots)


if __name__ == '__main__':
	unittest.main()
