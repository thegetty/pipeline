import unittest 
import sys
import os
import json
import pprint

from bonobo.config import Configurable, Option
import aata

def merge_lists(l, r):
	'''
	Given two lists `l` and `r`, return a generator of the combined items from both lists.
	
	If any two items l' in l and r' in r are both `dict`s and have the same value for the
	`id` key, they will be `merge`d and the resulting `dict` included in the results in
	place of l' or r'.
	'''
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
	'''
	Given two items `l` and `r` of the same type, merge their contents and return the
	result. Raise an exception if `l` and `r` are of differing types.
	
	If the items are of type `dict`, recursively merge any values with shared keys, and
	also include data from any non-shared keys. If `l` and `r` both have values for the
	`id` key and they differ in value, raise an exception.
	
	If the items are of type `list`, merge them with `merge_lists`.
	
	If the items are of type `str` or `bytes`, return the value if `l` and `r` are equal.
	Otherwise raise and exception.
	'''
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
		intersection = set([k for k in keys if k in l and k in r])
		if 'id' in intersection:
			lid = l['id']
			rid = r['id']
			if lid != rid:
				raise Exception('attempt to merge two dicts with different ids: (%r, %r)' % (lid, rid))
		return {k: merge(l.get(k), r.get(k)) for k in keys}
	elif type(l) == list:
		return list(merge_lists(l, r))
	elif type(l) in (str, bytes):
		if l == r:
			return l
		else:
			raise Exception('data conflict: %r <=> %r' % (l, r))
	else:
		raise NotImplemented('unhandled type: %r' % (type(l)))
	return l

class TestWriter(Configurable):
	'''
	Deserialize the output of each resource and store in memory.
	Merge data for multiple serializations of the same `uuid`.
	'''
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
	'''
	Test AATA pipeline subclass that allows using a custom Writer.
	'''
	def __init__(self, writer, input_path, files_pattern, output_path=None, models=None, limit=None, debug=False):
		super().__init__(input_path, files_pattern, models=models, limit=limit, debug=debug)
		self.writer = writer

	
class TestAATAPipelineOutput(unittest.TestCase):
	'''
	Parse test XML data and run the AATA pipeline with the in-memory TestWriter.
	Then verify that the serializations in the TestWriter object are what was expected.
	'''
	def setUp(self):
		self.files_pattern = 'tests/data/aata-sample1.xml'
		pass

	def tearDown(self):
		pass

	def test_pipeline_1(self):
		input_path = os.getcwd()
		print(f'*** {input_path}')
		writer = TestWriter()
		models = {
			'Person': '0b47366e-2e42-11e9-9018-a4d18cec433a',
			'LinguisticObject': 'model-lo',
			'Organization': 'model-org'
		}
		pipeline = AATATestPipeline(writer, input_path, self.files_pattern, models=models, limit=1, debug=True)
		pipeline.run()
		output = writer.output
		self.assertEqual(len(output), 3)

		lo_model = models['LinguisticObject']
		people_model = models['Person']
		orgs_model = models['Organization']

		expected_models = {
			people_model,
			lo_model,
			orgs_model
		}

		self.assertEqual(set(output.keys()), expected_models)
		self.assertEqual(len(output[people_model]), 2)
		self.assertEqual(len(output[lo_model]), 2)
		self.assertEqual(len(output[orgs_model]), 3)
		
		people = output[people_model].values()
		people_creation_events = set()
		for p in people:
			for e in p.get('carried_out', []):
				cid = e['id']
				people_creation_events.add(cid)
		people_names = sorted(p.get('_label') for p in people)
		self.assertEqual(people_names, ['Bremner, Ian', 'Meyers, Eric'])

		organizations = output[orgs_model].values()
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
		
		lo = output[lo_model].values()
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
