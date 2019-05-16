import unittest
import os
import json
import pprint
from collections import defaultdict

from pipeline.projects.aata import AATAPipeline

def merge_lists(l, r):
	'''
	Given two lists `l` and `r`, return a generator of the combined items from both lists.

	If any two items l' in l and r' in r are both `dict`s and have the same value for the
	`id` key, they will be `merge`d and the resulting `dict` included in the results in
	place of l' or r'.
	'''
	identified = {}
	all_items = l + r
	other = []
	for item in all_items:
		try:
			item_id = item.get('id')
			if item_id in identified:
				identified[item_id] += [item]
			else:
				identified[item_id] = [item]
		except:
			other.append(item)

	for items in identified.values():
		r = items[:]
		while len(r) > 1:
			a = r.pop(0)
			b = r.pop(0)
			m = merge(a, b)
			r.insert(0, m)
		yield from r

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

	if not isinstance(l, type(r)):
		pprint.pprint(l)
		pprint.pprint(r)
		raise Exception('bad data in json merge')

	if isinstance(l, dict):
		keys = set(list(l.keys()) + list(r.keys()))
		intersection = {k for k in keys if k in l and k in r}
		if 'id' in intersection:
			lid = l['id']
			rid = r['id']
			if lid != rid:
				raise Exception('attempt to merge two dicts with different ids: (%r, %r)' % (lid, rid))
		return {k: merge(l.get(k), r.get(k)) for k in keys}
	elif isinstance(l, list):
		return list(merge_lists(l, r))
	elif isinstance(l, (str, bytes)):
		if l == r:
			return l
		else:
			raise Exception('data conflict: %r <=> %r' % (l, r))
	else:
		raise NotImplementedError('unhandled type: %r' % (type(l)))
	return l

class TestWriter():
	'''
	Deserialize the output of each resource and store in memory.
	Merge data for multiple serializations of the same `uuid`.
	'''
	def __init__(self):
		self.output = {}
		super().__init__()

	def __call__(self, data: dict, *args, **kwargs):
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


class AATATestPipeline(AATAPipeline):
	'''
	Test AATA pipeline subclass that allows using a custom Writer.
	'''
	def __init__(self, writer, input_path, files_pattern, **kwargs):
		super().__init__(input_path, files_pattern, **kwargs)
		self.writer = writer


class TestAATAPipelineOutput(unittest.TestCase):
	'''
	Parse test XML data and run the AATA pipeline with the in-memory TestWriter.
	Then verify that the serializations in the TestWriter object are what was expected.
	'''
	def setUp(self):
		self.files_pattern = 'tests/data/aata-sample1.xml'
		os.environ['QUIET'] = '1'

	def tearDown(self):
		pass

	def run_pipeline(self, models, input_path):
		writer = TestWriter()
		pipeline = AATATestPipeline(
			writer,
			input_path,
			self.files_pattern,
			models=models,
			limit=1,
			debug=True
		)
		pipeline.run()
		output = writer.output
		return output

	def verify_people_for_AATA140375(self, output, people_model):
		people = output[people_model].values()
		people_creation_events = set()
		for p in people:
			for event in p.get('carried_out', []):
				cid = event['id']
				people_creation_events.add(cid)
		people_names = sorted(p.get('_label') for p in people)
		self.assertEqual(people_names, ['Bremner, Ian', 'Meyers, Eric'])
		return people_creation_events

	def verify_properties_AATA140375(self, data):
		abstract, article = data.values()
		article_classification = {l['_label'] for l in article['classified_as']}
		if 'Abstract' in article_classification:
			abstract, article = article, abstract
		
		self.assertIn('The Forbidden City in Beijing', abstract['content'])
		self.assertEqual('http://vocab.getty.edu/aat/300026032', abstract['classified_as'][0]['id']) # abstract
		self.assertEqual('AATA140375', abstract['identified_by'][0]['content'])
		self.assertEqual('Local Number', abstract['identified_by'][0]['classified_as'][0]['_label'])
		self.assertEqual('English', abstract['language'][0]['_label'])
		self.assertEqual('LinguisticObject', abstract['type'])
		
		abstracted_thing = abstract['refers_to'][0]
		abstracted_thing_id = abstracted_thing.get('id')
		article_id = article.get('id')
		self.assertEqual(article_id, abstracted_thing_id, 'Article and the abstracgted thing have the same ID')

		merged_thing = merge(article, abstracted_thing)
		self.assertIn('Secrets of the Forbidden City', merged_thing['_label'])
		self.assertEqual('http://vocab.getty.edu/aat/300028045', merged_thing['classified_as'][0]['id']) # AV
		self.assertEqual('LinguisticObject', merged_thing['type'])
		self.assertEqual('Creation', merged_thing['created_by']['type'])
		identifiers = defaultdict(set)
		for x in merged_thing['identified_by']:
			identifiers[x['classified_as'][0]['_label']].add(x['content'])
		self.assertEqual(dict(identifiers), {
			'Title': {'Secrets of the Forbidden City'},
			'ISBN Identifier': {'1531703461', '9781531703462'},
		})

		about = defaultdict(set)
		for x in merged_thing['about']:
			about[x['type']].add(x['_label'])
		self.assertEqual(about, {
			'Group': {'Palace Museum //Beijing (China)'},
			'Type': {
				'Ming',
				'Structural studies and consolidation of buildings',
				'brackets (structural elements)',
				'building materials',
				'construction techniques',
				'earthquakes',
				'experimentation',
				'historic structures (single built works)',
				'seismic design',
				'structural analysis'
			}
		})

	def verify_model_counts_for_AATA140375(self, output, lo_model, people_model, orgs_model):
		expected_models = {
			people_model,
			lo_model,
			orgs_model
		}
		self.assertEqual(set(output.keys()), expected_models)
		self.assertEqual(len(output[people_model]), 2)
		self.assertEqual(len(output[lo_model]), 2)
		self.assertEqual(len(output[orgs_model]), 3)

	def verify_organizations_for_AATA140375(self, output, orgs_model):
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

	def verify_data_for_AATA140375(self, output, lo_model):
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
				source_creation_events.remove(i)
		types = sorted(article_types.values())
		self.assertEqual(types, ['A/V Content', 'Abstract'])
		self.verify_properties_AATA140375(output[lo_model])
		return source_creation_events, abstract_creation_events

	def test_pipeline_with_AATA140375(self):
		input_path = os.getcwd()
		models = {
			'Person': '0b47366e-2e42-11e9-9018-a4d18cec433a',
			'LinguisticObject': 'model-lo',
			'Organization': 'model-org'
		}
		output = self.run_pipeline(models, input_path)
		self.assertEqual(len(output), 3)

		lo_model = models['LinguisticObject']
		people_model = models['Person']
		orgs_model = models['Organization']

		self.verify_model_counts_for_AATA140375(output, lo_model, people_model, orgs_model)
		people_creation_events = self.verify_people_for_AATA140375(output, people_model)
		self.verify_organizations_for_AATA140375(output, orgs_model)
		source_creation_events, abstract_creation_events = self.verify_data_for_AATA140375(output, lo_model)

		# the creation sub-events from both the abstract and the
		# thing-being-abstracted are carried out by the people
		self.assertLessEqual(source_creation_events, people_creation_events)
		self.assertLessEqual(abstract_creation_events, people_creation_events)
