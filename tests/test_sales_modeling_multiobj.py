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

from tests import TestSalesPipelineOutput
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_MultiObject(TestSalesPipelineOutput):
	def test_modeling_for_multi_object_lots(self):
		'''
		Test for modeling of lots containing multiple objects.
		'''
		output = self.run_pipeline('multiobj')

		objects = output['model-object']
		activities = list(output['model-activity'].values())
		sale_activities = list(output['model-sale-activity'].values())

		self.assertEqual(len(activities), 1)
		self.assertEqual(len(sale_activities), 2)
		
		act_by_type = {k: list(v) for k,v in groupby(activities, key=lambda a: a['classified_as'][0]['_label'])}
		self.assertEqual(set(act_by_type.keys()), {'Provenance Entry'})
		
		sale_by_type = {k: list(v) for k,v in groupby(sale_activities, key=lambda a: a['classified_as'][0]['_label'])}
		self.assertEqual(set(sale_by_type.keys()), {'Auction Event', 'Auction of Lot'})
		
		# there is 1 procurement
		self.assertEqual(len(act_by_type['Provenance Entry']), 1)
		procurement = act_by_type['Provenance Entry'][0]

		# there is 1 lot
		self.assertEqual(len(sale_by_type['Auction of Lot']), 1)
		lot = sale_by_type['Auction of Lot'][0]

		# procurement has 1 payment, 3 acquisitions, and 6 transfers of custody
		parts = procurement['part']
		proc_types = sorted([a['type'] for a in parts])
		self.assertEqual(proc_types, ['Acquisition', 'Acquisition', 'Acquisition', 'Payment', 'TransferOfCustody', 'TransferOfCustody', 'TransferOfCustody', 'TransferOfCustody', 'TransferOfCustody', 'TransferOfCustody'])
		acqs = [a for a in parts if a['type'] == 'Acquisition']
		paym = [a for a in parts if a['type'] == 'Payment'][0]

		# the objects in the 3 acquisitions are the same as the objects in the lot
		objects_from_acqs = set()
		for acq in acqs:
			for obj in acq.get('transferred_title_of', []):
				objects_from_acqs.add(obj['id'])
		
		object_sets_for_lots = set([s['id'] for s in lot.get('used_specific_object', [])])

		objects_from_lots = set()
		for obj in objects.values():
			for m in obj.get('member_of', []):
				if m['id'] in object_sets_for_lots:
					objects_from_lots.add(obj['id'])

		self.assertEqual(objects_from_acqs, objects_from_lots)


if __name__ == '__main__':
	unittest.main()
