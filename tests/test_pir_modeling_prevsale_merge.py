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
from cromulent import reader
from cromulent.model import factory

from pipeline.util import CromObjectMerger
from pipeline.util.rewriting import JSONValueRewriter
from pipeline.projects.provenance.util import prev_post_sales_rewrite_map
from tests import TestProvenancePipelineOutput
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_PrevSaleMerge(TestProvenancePipelineOutput):
	def merge_objects(self, objects):
		rewrite_map_data = self.prev_post_sales_map
		post_sale_rewrite_map = prev_post_sales_rewrite_map(rewrite_map_data)
		r = JSONValueRewriter(post_sale_rewrite_map)
		for k in list(objects.keys()):
			data = objects[k]
			updated = r.rewrite(data)
			ident = updated['id']
			if k != ident:
				if ident in objects:
					read = reader.Reader()
					m = read.read(json.dumps(objects[ident]))
					n = read.read(json.dumps(updated))
					merger = CromObjectMerger()
					m = merger.merge(m, n)
					objects[ident] = json.loads(factory.toString(m, False))
				else:
					objects[ident] = updated
				del(objects[k])

	def test_modeling_prev_post_sales_merge(self):
		'''
		Test that prev/post sales data is collected properly during the pipeline run,
		and that it leads to correctly merged HumanMadeObjects after post-processing.
		'''
		output = self.run_pipeline('prevsale_merge')
		objects = output['model-object']

		# after the pipeline run, 2 objects are recorded as being the same as a prev sale
		# object (indicating that after merging there will be either 1 or 2 objects).
		self.assertEqual(len(self.prev_post_sales_map), 2)

		# there are records for 3 objects, and each is a member of only one auction lot,
		# and all auction lots are distinct
		self.assertEqual(len(objects), 3)
		auction_lots = set([l['id'] for hmo in objects.values() for l in hmo['member_of']])
		self.assertEqual(len(auction_lots), 3)
		for hmo in objects.values():
			self.assertEqual(len(hmo['member_of']), 1)

		# after merging prev/post sales, there is only one object
		self.merge_objects(objects)
		self.assertEqual(len(objects), 1)

		# and the single object is a member of all 3 auction lots
		hmo = next(iter(objects.values()))
		auction_lots = set([l['id'] for l in hmo['member_of']])
		self.assertEqual(len(auction_lots), 3)


if __name__ == '__main__':
	unittest.main()
