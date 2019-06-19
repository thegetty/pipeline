import unittest
import os
import os.path
import hashlib
import json
import pprint
from collections import defaultdict

from pipeline.projects.provenance import ProvenancePipeline

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
				pprint.pprint(l)
				pprint.pprint(r)
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
	Merge data for multiple serializations of the same resource.
	'''
	def __init__(self):
		self.output = {}
		super().__init__()

	def __call__(self, data: dict, *args, **kwargs):
		d = data['_OUTPUT']
		dr = data['_ARCHES_MODEL']
		if dr not in self.output:
			self.output[dr] = {}
		uu = data.get('uuid')
		if not uu:
			uu = hashlib.sha256(data['uri'].encode('utf-8')).hexdigest()
			print(f'*** No UUID in top-level resource. Using an assigned SHA256 filename for the content: {uu}.json')
		fn = '%s.json' % uu
		data = json.loads(d)
		if fn in self.output[dr]:
			self.output[dr][fn] = merge(self.output[dr][fn], data)
		else:
			self.output[dr][fn] = data


class ProvenanceTestPipeline(ProvenancePipeline):
	'''
	Test Provenance pipeline subclass that allows using a custom Writer.
	'''
	def __init__(self, writer, input_path, catalogs, auction_events, contents, **kwargs):
		super().__init__(input_path, catalogs, auction_events, contents, **kwargs)
		self.writer = writer


class TestProvenancePipelineOutput(unittest.TestCase):
	'''
	Parse test CSV data and run the Provenance pipeline with the in-memory TestWriter.
	Then verify that the serializations in the TestWriter object are what was expected.
	'''
	def setUp(self):
		self.catalogs = {
			'header_file': 'tests/data/pir/sales_catalogs_info_0.csv',
			'files_pattern': 'tests/data/pir/sales_catalogs_info.csv',
		}
		self.contents = {
			'header_file': 'tests/data/pir/sales_contents_0.csv',
			'files_pattern': 'tests/data/pir/sales_contents_1.csv',
		}
		self.auction_events = {
			'header_file': 'tests/data/pir/sales_descriptions_0.csv',
			'files_pattern': 'tests/data/pir/sales_descriptions.csv',
		}
		os.environ['QUIET'] = '1'

	def tearDown(self):
		pass

	def run_pipeline(self, models, input_path):
		writer = TestWriter()
		pipeline = ProvenanceTestPipeline(
				writer,
				input_path,
				catalogs=self.catalogs,
				auction_events=self.auction_events,
				contents=self.contents,
				models=models,
				limit=10,
				debug=True
		)
		pipeline.run()
		output = writer.output
		return output

	def verify_auction(self, a, event, idents):
		got_events = {c['_label'] for c in a.get('part_of', [])}
		self.assertEqual(got_events, {f'Auction Event for {event}'})
		got_idents = {c['content'] for c in a.get('identified_by', [])}
		self.assertEqual(got_idents, idents)

	def test_pipeline_pir(self):
		input_path = os.getcwd()
		models = {
			'ManMadeObject': 'model-object',
			'LinguisticObject': 'model-lo',
			'Person': 'model-person',
			'Auction': 'model-auction',
			'AuctionHouseOrg': 'model-auctionhouse',
			'Activity': 'model-activity',
		}
		output = self.run_pipeline(models, input_path)

		objects = output['model-object']
		los = output['model-lo']
		people = output['model-person']
		auctions = output['model-auction']
		houses = output['model-auctionhouse']
		activities = output['model-activity']
		
		self.assertEqual(len(people), 3, 'expected count of people')
		self.assertEqual(len(objects), 6, 'expected count of physical objects')
		self.assertEqual(len(los), 1, 'expected count of linguistic objects')
		self.assertEqual(len(auctions), 2, 'expected count of auctions')
		self.assertEqual(len(houses), 1, 'expected count of auction houses')
		self.assertEqual(len(activities), 4, 'expected count of activities')

		object_types = {c['_label'] for o in objects.values() for c in o.get('classified_as', [])}
		self.assertEqual(object_types, {'Painting'})

		lo_types = {c['_label'] for o in los.values() for c in o.get('classified_as', [])}
		self.assertEqual(lo_types, {'Auction Catalog'})
		
		people_names = {o['_label'] for o in people.values()}
		self.assertEqual(people_names, {'[Anonymous]', 'Gillemans', 'Vinckebooms'})
		
		auction_B_A139_0119 = auctions['84c2bf192917c905f6e1189a7fa28ad47d2612979a149122163cd281afe53c41.json']
		self.verify_auction(auction_B_A139_0119, event='B-A139', idents={'0119[a]', '0119[b]'})

		auction_B_A139_0120 = auctions['14ae49cefb2a2def33a8e01fc8ec26358165bbcf2f15eee35353a311522a258d.json']
		self.verify_auction(auction_B_A139_0120, event='B-A139', idents={'0120'})
		
		house_names = {o['_label'] for o in houses.values()}
		house_types = {c['_label'] for o in houses.values() for c in o.get('classified_as', [])}
		self.assertEqual(house_names, {'Paul de Cock'})
		self.assertEqual(house_types, {'Auction House (organization)'})

		activity_labels = {o['_label'] for o in activities.values()}
		self.assertEqual(activity_labels, {'Bidding on B-A139 0119[a]', 'Bidding on B-A139 0119[b]', 'Bidding on B-A139 0120', 'Auction Event for B-A139'})

if __name__ == '__main__':
	unittest.main()
