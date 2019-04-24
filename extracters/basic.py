# Extracters

from bonobo.config import Configurable, Service, Option, use
import sys
import uuid
import copy
import pprint
import difflib

# ~~~~ Core Functions ~~~~

aat_label_cache = {}

class AddArchesModel(Configurable):
	model = Option()
	def __call__(self, data):
		data['_ARCHES_MODEL'] = self.model
		return data

class AddFieldNames(Configurable):
	key = Option()
	field_names = Option()
	def __call__(self, *data):	
		if len(data) == 1 and type(data[0]) == tuple:
			data = data[0]
		names = self.field_names.get(self.key, [])
		return dict(zip(names, data))		

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

# This is so far just internal
@use('uuid_cache')
def add_uuid(thing: dict, uuid_cache=None):
	# Need access to select from the uuid_cache
	uid = thing['uid']
	s = 'SELECT uuid FROM mapping WHERE key = :uid'
	res = uuid_cache.execute(s, uid=uid)
	row = res.fetchone()
	if row is None:
		uu = str(uuid.uuid4())
		c = 'INSERT OR IGNORE INTO mapping (key, uuid) VALUES (:uid, :uuid)'
		uuid_cache.execute(c, uid, uu)
		thing['uuid'] = uu
		res = uuid_cache.execute(s, uid=uid)
		row = res.fetchone()
		if row is None:
			raise Exception('Failed to add and access a new UUID for key %r' % (uid,))
	thing['uuid'] = row[0]
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
	ordinals = Option(default=(1,))
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
