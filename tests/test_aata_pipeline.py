import unittest 
import sys
import os
import json
import pprint

from bonobo.config import Configurable, Option
import aata

def merge_lists(l, r):
	identified = {}
	all = l + r
	other = []
	for item in all:
		try:
			id = i.get('id')
			if id in identified:
				identified[id] += [item]
			else:
				identified[id] = [item]
		except:
			other.append(item)

	for items in identified:
		yield merge(*items)
	
	yield from other

def merge(l, r):
	if l is None:
		return r
	if r is None:
		return l

	if type(l) != type(r):
		pprint.pprint(l)
		pprint.pprint(r)
		raise Exception('bad data in json merge')
	
	if type(l) == dict:
		keys = set(list(l.keys()) + list(r.keys()))
		return {k: merge(l.get(k), r.get(k)) for k in keys}
	elif type(l) == list:
		return list(merge_lists(l, r))
	elif type(l) == str:
		if l == r:
			return l
		else:
			raise Exception('data conflict: %r <=> %r' % (l, r))
	else:
		raise Exception('unhandled type: %r' % (type(l)))
	return l

class TestWriter(Configurable):
	def __init__(self):
		self.output = {}

	def __call__(self, data: dict):
		d = data['_OUTPUT']
		dr = data['_ARCHES_MODEL']
		if dr not in self.output:
			self.output[dr] = {}
		fn = '%s.json' % data['uuid']
		data = json.loads(d)
		if fn in self.output[dr]:
			self.output[dr][fn] = merge(self.output[dr][fn], data)
		else:
			self.output[dr][fn] = data


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

		lo_model = '41a41e47-2e42-11e9-b5ee-a4d18cec433a'
		people_model = '0b47366e-2e42-11e9-9018-a4d18cec433a'
		orgs_model = 'XXX-Organization-Model'

		expected_models = {
			people_model,
			lo_model,
			orgs_model
		}

# 		pprint.pprint(output[people_model])
		self.assertEqual(set(output.keys()), expected_models)
		self.assertEqual(len(output[people_model]), 2)
		self.assertEqual(len(output[lo_model]), 2)
		self.assertEqual(len(output[orgs_model]), 3)
		
		people = [s for s in output[people_model].values()]
		people_creation_events = set()
		for p in people:
			for e in p.get('carried_out', []):
				cid = e['id']
				people_creation_events.add(cid)
		people_names = sorted(p.get('_label') for p in people)
		self.assertEqual(people_names, ['Bremner, Ian', 'Meyers, Eric'])

		organizations = [s for s in output[orgs_model].values()]
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
		
		lo = [s for s in output[lo_model].values()]
		article_types = {}
		source_creation_events = set()
		abstract_creation_events = set()
		for a in lo:
			i = a['id']
			try:
				article_types[i] = a['classified_as'][0]['_label']
			except Exception as e:
				print('*** error while handling linguistic object classification: %s' % (e,))
				article_types[i] = None
			try:
				if 'created_by' in a:
					if a['classified_as'][0]['_label'] == 'Abstract':
						c = a['created_by']
						cid = c['id']
						for p in c.get('part', []):
							abstract_creation_events.add(p.get('id'))
				for thing in a.get('refers_to', []):
					if 'created_by' in thing:
						event = thing['created_by']
						for p in event.get('part', []):
							source_creation_events.add(p.get('id'))
			except Exception as e:
				print('*** error while handling creation event: %s' % (e,))
				pprint.pprint(c)
				source_creation_events[i] = None
		types = sorted(article_types.values())
		self.assertEqual(types, ['A/V Content', 'Abstract'])

		# the creation sub-events from both the abstract and the
		# thing-being-abstracted are carried out by the people
		self.assertLessEqual(source_creation_events, people_creation_events)
		self.assertLessEqual(abstract_creation_events, people_creation_events)
