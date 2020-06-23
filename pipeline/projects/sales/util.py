import itertools
import urllib.parse
from collections import Counter
import uuid
import json
import warnings

from pipeline.util import implode_date
from pipeline.projects import UtilityHelper

UID_TAG_PREFIX = 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#'

def filter_empty_person(data: dict, _):
	'''
	If all the values of the supplied dictionary are false (or false after int conversion
	for keys ending with 'ulan'), return `None`. Otherwise return the dictionary.
	'''
	set_flags = []
	for k, v in data.items():
		if k.endswith('ulan'):
			if v in ('', '0'):
				s = False
			else:
				s = True
		elif k in ('pi_record_no', 'star_rec_no'):
			s = False
		else:
			s = bool(v)
		set_flags.append(s)
	if any(set_flags):
		return data
	else:
		return None

def add_pir_record_ids(data, parent):
	'''Copy identifying key-value pairs from `parent` to `data`, returning `data`'''
	for p in ('pi_record_no', 'persistent_puid'):
		data[p] = parent.get(p)
	return data

def object_key_string(cno, lno, date):
	return f'{cno} {lno} ({date})'

def object_key(data):
	'''
	Returns a 3-tuple of (catalog number, lot number, sale date) that identify an object
	from a sales record, extracted from properties of the supplied `data` dict.
	'''
	cno = data['catalog_number']
	lno = data['lot_number']
	date = implode_date(data, 'lot_sale_')
	return (cno, lno, date)

def object_uri(data, helper):
	key = object_key(data)
	return helper.make_proj_uri('OBJ', *key)

def add_pir_object_uri_factory(helper):
	return lambda d, p: add_pir_object_uri(d, p, helper)

def add_pir_object_uri(data, parent, helper):
	'''
	Set 'uri' keys in `data` based on its identifying properties, returning `data`.
	'''
	add_pir_record_ids(data, parent)
	data['uri'] = object_uri(parent, helper)
	return data

class SalesTree:
	'''
	This class is used to represent the repeated sales of objects in sales data.
	It stores a graph of trees where each node is a lot sale, and edges connect sales
	of the same object over time.

	The `post_sale_map` data that is generated during the pipeline run is used to identify
	lots which contain just a single object. When this data indicates that a single object
	was sold multiple times, links are added to this `SalesTree` by a call to `add_edge`
	using the lot keys.

	Subsequently, `canonical_key` is used to return a single key for each sales record
	that belongs to a connected component in the `SalesTree` graph. This canonical key
	is used in a post-processing phase (based on the `post_sale_rewrite_map` file) that
	rewrites many URLs in the output data which all identify a single object to a single
	URL.
	'''
	def __init__(self):
		self.counter = itertools.count()
		self.nodes = {}
		self.nodes_rev = {}
		self.outgoing_edges = {}

	def add_node(self, node):
		if node not in self.nodes:
			i = next(self.counter)
			self.nodes[node] = i
			self.nodes_rev[i] = node
		i = self.nodes[node]
		return i

	def largest_component_canonical_keys(self, limit=None):
		helper = UtilityHelper('sales')
		components = Counter()
		for src in self.nodes.keys():
			key, _ = self.canonical_key(src)
			components[key] += 1
# 		print(f'Post sales records connected component sizes (top {limit}):')
		for key, count in components.most_common(limit):
			uri = helper.make_proj_uri('OBJ', *key)
# 			print(f'{count}\t{key!s:>40}\t\t{uri}')
			yield key

	def add_edge(self, src, dst):
		i = self.add_node(src)
		j = self.add_node(dst)
# 		if i in self.outgoing_edges:
# 			if self.outgoing_edges[i] == j:
# 				warnings.warn(f'*** re-asserted sale edge: {src!s:<40} -> {dst}')
# 			else:
# 				warnings.warn(f'*** {src} already has an outgoing edge: {self.outgoing_edges[i]}')
		self.outgoing_edges[i] = j

	def __iter__(self):
		for i in self.outgoing_edges.keys():
			j = self.outgoing_edges[i]
			src = self.nodes_rev[i]
			dst = self.nodes_rev[j]
			yield (src, dst)

	@staticmethod
	def load(f):
		d = json.load(f)
		g = SalesTree()
		g.counter = itertools.count(d['next'])
		g.nodes_rev = {int(i): tuple(n) for i, n in d['nodes'].items()}
		g.nodes = {n: i for i, n in g.nodes_rev.items()}
		g.outgoing_edges = {int(k): int(v) for k, v in d['outgoing'].items()}
		return g

	def dump(self, f):
		nodes = {i: list(n) for n, i in self.nodes.items()}
		d = {
			'next': next(self.counter),
			'nodes': nodes,
			'outgoing': self.outgoing_edges,
		}
		json.dump(d, f)

	def canonical_key(self, src):
		key = src
		steps = 0
		seen = {key}
		path = [key]
		while True:
			if key not in self.nodes:
				break
			i = self.nodes[key]
			if i not in self.outgoing_edges:
				break
			j = self.outgoing_edges[i]

			parent = self.nodes_rev[j]
			if parent == key:
				warnings.warn(f'*** Self-loop found in post sale data: {key!s:<40}')
				break
			if parent in seen:
				path.append(parent)
				warnings.warn(f'*** Loop found in post sale data: {path}')
# 				warnings.warn(f'*** Loop found in post sale data: {key!s:<40} -> {parent!s:<40}')
				break
			key = parent
			seen.add(key)
			path.append(key)
			steps += 1
		return key, steps
