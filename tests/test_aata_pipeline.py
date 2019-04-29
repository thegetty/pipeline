import unittest 
import sys
import os
import json
import pprint

from bonobo.config import Configurable, Option
import aata

class TestWriter(Configurable):
	def __init__(self):
		self.output = {}

	def __call__(self, data: dict):
		d = data['_OUTPUT']
		dr = data['_ARCHES_MODEL']
		if dr not in self.output:
			self.output[dr] = {}
		fn = '%s.json' % data['uuid']
		if fn in self.output[dr]:
			raise Exception('Output already exists for %r' % (fn,))
		self.output[dr][fn] = d


class AATATestPipeline(aata.AATAPipeline):
	def __init__(self, writer, input_path, files, output_path=None, limit=None, debug=False):
		super().__init__(input_path, files, limit=limit, debug=debug)
		self.writer = writer

	
class TestAATAPipelineOutput(unittest.TestCase):
	def setUp(self):
		self.files = ['tests/data/aata-sample1.xml']
		pass

	def tearDown(self):
		pass

	def test_pipeline_1(self):
		input_path = ''
		writer = TestWriter()
		pipeline = AATATestPipeline(writer, input_path, self.files, limit=1, debug=True)
		pipeline.run()
		output = writer.output
		self.assertEqual(len(output), 3)

		articles_model = '41a41e47-2e42-11e9-b5ee-a4d18cec433a'
		people_model = '0b47366e-2e42-11e9-9018-a4d18cec433a'
		orgs_model = 'XXX-Organization-Model'

		expected_models = {
			people_model,
			articles_model,
			orgs_model
		}
		self.assertEqual(set(output.keys()), expected_models)
		self.assertEqual(len(output[people_model]), 2)
		self.assertEqual(len(output[articles_model]), 3)
		self.assertEqual(len(output[orgs_model]), 3)
		
		people = [json.loads(s) for s in output[people_model].values()]
		people_names = sorted(p.get('_label') for p in people)
		self.assertEqual(people_names, ['Bremner, Ian', 'Meyers, Eric'])

		organizations = [json.loads(s) for s in output[orgs_model].values()]
		org_names = {}
		for o in organizations:
			try:
				i = o['id']
				l = o.get('_label')
				org_names[i] = l
			except Exception as e:
				print('*** %s' % (e,))
				org_names[i] = None
		self.assertEqual(sorted(org_names.values()), [
			'Lion Television USA //New York (New York, United States)',
			'Public Broadcasting Associates, Inc. //Boston (Massachusetts, United States)',
			'WGBH Educational Foundation //Boston (Massachusetts, United States)'
		])
		
		articles = [json.loads(s) for s in output[articles_model].values()]
		article_types = {}
		for a in articles:
			pprint.pprint(a)
			try:
				i = a['id']
				cl = a.get('classified_as')
				c = cl[0]
				l = c.get('_label')
				article_types[i] = l
			except Exception as e:
				print('*** %s' % (e,))
				article_types[i] = None
		pprint.pprint(article_types)
		types = sorted(article_types.values())
		self.assertEqual(types, ['A/V Content', 'Abstract'])
