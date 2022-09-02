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

class PIRModelingTest_MultiLot(TestSalesPipelineOutput):
	def test_modeling_for_multi_lot_procurements(self):
		'''
		Test for modeling of single procurements for which a single payment was made
		for multiple auction lots.
		'''
		output = self.run_pipeline('multilot')

		objects = output['model-object']
		auctions = list(output['model-sale-activity'].values())
		activities = list(output['model-activity'].values())
		sales = [a for a in activities if a.get('_label', '').startswith('Sale')]
		self.assertEqual(len(sales), 1)
		self.assertEqual(len(auctions), 2)
		
		by_type = {k: list(v) for k,v in groupby(sales, key=lambda a: a['classified_as'][0]['_label'])}
		self.assertEqual(sorted(by_type.keys()), ['Provenance Activity'])

		# there are 2 lots and 1 procurement
		self.assertEqual(len(by_type['Provenance Activity']), 1)
		procurement = by_type['Provenance Activity'][0]


		by_type = {k: list(v) for k,v in groupby(auctions, key=lambda a: a['classified_as'][0]['_label'])}
		self.assertEqual(sorted(by_type.keys()), ['Auction of Lot'])
		self.assertEqual(len(by_type['Auction of Lot']), 2)
		lots = by_type['Auction of Lot']




		# procurement has 1 payment, 2 acquisitions, and 4 transfers of custody
		parts = procurement['part']
		proc_types = sorted([a['type'] for a in parts])
		self.assertEqual(proc_types, ['Acquisition', 'Acquisition', 'AttributeAssignment', 'AttributeAssignment', 'Payment', 'Payment', 'TransferOfCustody', 'TransferOfCustody', 'TransferOfCustody', 'TransferOfCustody'])
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
