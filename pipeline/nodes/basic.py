# Extracters

from bonobo.config import Configurable, Service, Option, Exclusive, use
import sys
import uuid
import copy
import pprint
import difflib
import functools
import itertools
from contextlib import suppress
from pipeline.util.cleaners import date_cleaner

# ~~~~ Core Functions ~~~~

aat_label_cache = {}

class CleanDateToSpan(Configurable):
	'''
	Supplied with a key name, attempt to parse the value in `input[key]`` as a date or
	date range, and create a new `TimeSpan` object for the parsed date(s). Store the
	resulting timespan in `input[key + '_span']`.
	'''

	key = Option(str, required=True)
	optional = Option(bool, default=True)

	def __init__(self, *v, **kw):
		'''
		Sets the __name__ property to include the relevant options so that when the
		bonobo graph is serialized as a GraphViz document, different objects can be
		visually differentiated.
		'''
		super().__init__(*v, **kw)
		self.__name__ = f'{type(self).__name__} ({self.key})'


	@staticmethod
	def string_to_span(value):
		'''Parse a string value and attempt to create a corresponding `model.TimeSpan` object.'''
		try:
			date_from, date_to = date_cleaner(value)
			ts = model.TimeSpan()
			if date_from is not None:
				ts.begin_of_the_begin = date_from.strftime("%Y-%m-%dT%H:%M:%SZ")
			if date_to is not None:
				ts.end_of_the_end = date_to.strftime("%Y-%m-%dT%H:%M:%SZ")
			return ts
		except Exception as e:
			print('*** Unknown date format %r: %s' % (value, e))
			return None

	def __call__(self, data, *args, **kwargs):
		if self.key in data:
			value = data[self.key]
			ts = self.string_to_span(value)
			if ts is not None:
				data['%s_span' % self.key] = ts
				return data
		else:
			if not self.optional:
				print('*** key %r is not in the data object:' % (self.key,))
				pprint.pprint(data)
		return NOT_MODIFIED

class GroupRepeatingKeys(Configurable):
	mapping = Option(dict)
	drop_empty = Option(bool, default=True)
	def __call__(self, data):
		d = data.copy()
		for key, mapping in self.mapping.items():
			property_prefixes = mapping['prefixes']
			postprocess = mapping.get('postprocess')
			d[key] = []
			with suppress(KeyError):
				for i in itertools.count(1):
					ks = ((prefix, f'{prefix}_{i}') for prefix in property_prefixes)
					subd = {}
					for p, k in ks:
						subd[p] = d[k]
						del d[k]
					if self.drop_empty:
						values_unset = list(map(lambda v: not bool(v), subd.values()))
						if all(values_unset):
							continue
					if postprocess:
						if callable(postprocess):
							postprocess = [postprocess]
						for p in postprocess:
							subd = p(subd, data)
					d[key].append(subd)
		return d

class GroupKeys(Configurable):
	mapping = Option(dict)
	drop_empty = Option(bool, default=True)
	def __call__(self, data):
		d = data.copy()
		for key, mapping in self.mapping.items():
			subd = {}
			properties = mapping['properties']
			postprocess = mapping.get('postprocess')
			for k in properties:
				v = d[k]
				del d[k]
				if self.drop_empty and not v:
					continue
				subd[k] = v
			if postprocess:
				if callable(postprocess):
					postprocess = [postprocess]
				for p in postprocess:
					subd = p(subd, data)
			d[key] = subd
		return d

class AddDataDependentArchesModel(Configurable):
	'''
	Set the `_ARCHES_MODEL` key in the supplied `dict` to the appropriate arches model UUID
	and return it.
	'''
	models = Option()
	def __call__(self, data, *args, **kwargs):
		if '_LOD_OBJECT' in data:
			obj = data['_LOD_OBJECT']
			t = type(obj)
			tname = t.__name__
			if tname in self.models:
				data['_ARCHES_MODEL'] = self.models[tname]
				return data
			else:
				typename = type(obj).__name__
				if tname in self.models:
					data['_ARCHES_MODEL'] = self.models[typename]
					return data
				else:
					print(f'*** No Arches model available for {typename}')
				data['_ARCHES_MODEL'] = f'XXX-{typename}'
		else:
			data['_ARCHES_MODEL'] = self.models['LinguisticObject']
		return data

class AddArchesModel(Configurable):
	model = Option()
	def __call__(self, data):
		data['_ARCHES_MODEL'] = self.model
		return data

class AddFieldNames(Configurable):
	key = Option(required=False)
	field_names = Option()
	def __call__(self, *data):
		if len(data) == 1 and type(data[0]) in (tuple, list):
			data = data[0]
		names = self.field_names.get(self.key, []) if isinstance(self.field_names, dict) else self.field_names
		d = dict(zip(names, data))
		return d

class Offset(Configurable):
	offset = Option()
	seen = 0
	def __call__(self, *data):
		self.seen += 1
		if self.seen <= self.offset:
			return None
		else:
			return data

def deep_copy(data):
	new = {}
	# Not actually very deep, just shallow copies everything except
	# the object generated in another thread
	for (k,v) in data.items():
		if k != "_LOD_OBJECT":
			new[k] = v
	return new

def get_actor_type(ulan, uuid_cache, default="Actor"):
	if not ulan:
		return "Actor"
	s = 'SELECT type FROM actor_type WHERE ulan = :ulan'
	res = uuid_cache.execute(s, ulan=ulan)
	v = res.fetchone()
	if v:
		return v[0]
	else:
		return default

def fetch_uuid(key, uuid_cache):
	return add_uuid({'uid':key}, uuid_cache=uuid_cache)['uuid']

@functools.lru_cache(maxsize=128000)
def get_or_set_uuid(uid, uuid_cache):
	with Exclusive(uuid_cache):
		s = 'SELECT uuid FROM mapping WHERE key = :uid'
		res = uuid_cache.execute(s, uid=uid)
		row = res.fetchone()
		if row is None:
			uu = str(uuid.uuid4())
			c = 'INSERT OR IGNORE INTO mapping (key, uuid) VALUES (:uid, :uuid)'
			uuid_cache.execute(c, uid, uu)
			res = uuid_cache.execute(s, uid=uid)
			row = res.fetchone()
			if row is None:
				raise Exception('Failed to add and access a new UUID for key %r' % (uid,))
			return uu
		else:
			uu = row[0]
		return uu

# This is so far just internal
@use('uuid_cache')
def add_uuid(thing: dict, uuid_cache=None):
	if 'uuid' not in thing:
		# Need access to select from the uuid_cache
		uid = thing['uid']
		uuid = get_or_set_uuid(uid, uuid_cache)
		thing['uuid'] = uuid
	return thing

@use('aat')
def get_aat_label(term, aat=None):
	if term in aat_label_cache:
		return aat_label_cache[term]
	else:
		res = aat.execute('SELECT aat_label FROM aat WHERE aat_id = :id', id=term)
		l = res.fetchone()
		if l:
			aat_label_cache[term] = l[0]
		else:
			print("Got no hit in matt's aat table for %s" % term)
			print("Implement lookup to AAT via http")
			return "XXX - FIX ME"
		return l[0]

class Trace(Configurable):
	name = Option()
	diff = Option(default=False)
	ordinals = Option(default=(0,))
	trace_counter = Service('trace_counter')

	def __call__(self, thing: dict, trace_counter):
		key = '__trace_id'
		skey = '__trace_seq'
		if not key in thing:
			thing[key] = next(trace_counter)
			thing[skey] = 1
		else:
			thing[skey] += 1
		id = thing[key]
		seq = thing[skey]
		if id in self.ordinals:
			formatted = pprint.pformat({k: v for k, v in thing.items() if not k.startswith('__trace_')})
			if formatted[0] == '{' and formatted[-1] == '}':
				# adding newlines and a trailing comma helps with making a sensible diff
				formatted = '{\n ' + formatted[1:-1] + ',\n}\n'
			if self.diff:
				previous = thing.get('__trace_%d_%d' % (id, seq-1))
				print('===========> %s #%d: sequence %d' % (self.name, id, seq))
				if previous:
					lines = difflib.ndiff(previous.splitlines(keepends=True), formatted.splitlines(keepends=True))
					sys.stdout.writelines(lines)
				else:
					print(formatted)
			else:
				print(formatted)
			thing['__trace_%d_%d' % (id, seq)] = formatted
		return thing


### Linked Art related functions

class Serializer(Configurable):
	compact = Option(default=True)
	def __call__(self, data: dict):
		factory = data['_CROM_FACTORY']
		js = factory.toString(data['_LOD_OBJECT'], self.compact)
		data['_OUTPUT'] = js
		return data

def print_jsonld(data: dict):
	print(data['_OUTPUT'])
	return data
