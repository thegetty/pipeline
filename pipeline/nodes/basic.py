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
from cromulent import model
from pipeline.linkedart import get_crom_object

# ~~~~ Core Functions ~~~~

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

class KeyManagement(Configurable):
	operations = Option(list)
	drop_empty = Option(bool, default=True)

	def __call__(self, data:dict):
		pass
# 		print('KeyManagement')
		for step, op_record in enumerate(self.operations):
			pass
# 			print(f'- step {step}')
			for op, op_data in op_record.items():
				pass
# 				print(f'  - {op}')
				if op == 'remove':
					for key in op_data:
						with suppress(KeyError):
							del data[key]
				elif op == 'rename':
					for k, v in op_data.items():
						with suppress(KeyError):
							value = data[k]
							data[v] = value
							del data[k]
				elif op == 'group':
					to_delete = set()
					for key, mapping in op_data.items():
						subd = {}
						properties = mapping['properties']
						postprocess = mapping.get('postprocess')
						rename = mapping.get('rename_keys', {})
						for k in properties:
							v = data.get(k)
							to_delete.add(k)
							if self.drop_empty and not v:
								continue
							sub_key = rename.get(k, k)
							subd[sub_key] = v
						if postprocess:
							if callable(postprocess):
								postprocess = [postprocess]
							for p in postprocess:
								subd = p(subd, data)
						data[key] = subd
					for k in to_delete:
						with suppress(KeyError):
							del data[k]
				elif op == 'group_repeating':
					for key, mapping in op_data.items():
						property_prefixes = mapping['prefixes']
						postprocess = mapping.get('postprocess')
						rename = mapping.get('rename_keys', {})
						data[key] = []
						to_delete = set()
						with suppress(KeyError):
							for i in itertools.count(1):
								ks = ((prefix, f'{prefix}_{i}') for prefix in property_prefixes)
								subd = {}
								for p, k in ks:
									sub_key = rename.get(p, p)
									subd[sub_key] = data[k]
									to_delete.add(k)
								if self.drop_empty:
									values_unset = list(map(lambda v: not bool(v), subd.values()))
									if all(values_unset):
										continue
								if postprocess and subd:
									if callable(postprocess):
										postprocess = [postprocess]
									for p in postprocess:
										subd = p(subd, data)
										if not subd:
											break
								if subd:
									data[key].append(subd)
						for k in to_delete:
							del data[k]
				else:
					warnings.warn(f'Unrecognized operator {op!r} in KeyManagement')
		return data

class RemoveKeys(Configurable):
	keys = Option(set)
	def __call__(self, data:dict):
		for key in self.keys:
			with suppress(KeyError):
				del data[key]
		return data

class GroupRepeatingKeys(Configurable):
	mapping = Option(dict)
	drop_empty = Option(bool, default=True)
	def __call__(self, data):
		for key, mapping in self.mapping.items():
			property_prefixes = mapping['prefixes']
			postprocess = mapping.get('postprocess')
			data[key] = []
			to_delete = set()
			with suppress(KeyError):
				for i in itertools.count(1):
					ks = ((prefix, f'{prefix}_{i}') for prefix in property_prefixes)
					subd = {}
					for p, k in ks:
						subd[p] = data[k]
						to_delete.add(k)
					if self.drop_empty:
						values_unset = list(map(lambda v: not bool(v), subd.values()))
						if all(values_unset):
							continue
					if postprocess and subd:
						if callable(postprocess):
							postprocess = [postprocess]
						for p in postprocess:
							subd = p(subd, data)
							if not subd:
								break
					if subd:
						data[key].append(subd)
			for k in to_delete:
				del data[k]
		return data

class GroupKeys(Configurable):
	mapping = Option(dict)
	drop_empty = Option(bool, default=True)
	def __call__(self, data):
		to_delete = set()
		for key, mapping in self.mapping.items():
			subd = {}
			properties = mapping['properties']
			postprocess = mapping.get('postprocess')
			for k in properties:
				v = data.get(k)
				to_delete.add(k)
				if self.drop_empty and not v:
					continue
				subd[k] = v
			if postprocess:
				if callable(postprocess):
					postprocess = [postprocess]
				for p in postprocess:
					subd = p(subd, data)
			data[key] = subd
		for k in to_delete:
			with suppress(KeyError):
				del data[k]
		return data

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

class AddFieldNamesSimple(Configurable):
	field_names = Option()

	def __call__(self, data):
		d = {}
		names = self.field_names
		for i in range(len(names)):
			d[names[i]] = data[i]
		return d

class AddFieldNamesService(Configurable):
	key = Option(required=False) # This is passed into __init__ as a kwarg but not into __call__
	field_names = Service('header_names')   # This is passed into __call__ as a kwarg not at __init__  
	# ... go figure

	def __init__(self, *args, **kwargs):
		'''
		Sets the __name__ property to include the relevant options so that when the
		bonobo graph is serialized as a GraphViz document, different objects can be
		visually differentiated.
		'''
		super().__init__(self, *args, **kwargs)
		self.__name__ = f'{type(self).__name__} ({self.key})'

	def __call__(self, *data, field_names={}):
		if len(data) == 1 and type(data[0]) in (tuple, list):
			data = data[0]
		names = field_names.get(self.key, []) if isinstance(field_names, dict) else field_names
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

class OnlyCromModeledRecords:
	def __call__(self, data):
		o = get_crom_object(data)
		if o:
			yield data

class OnlyRecordsOfType(Configurable):
	type = Option()
	def __init__(self, *v, **kw):
		'''
		Sets the __name__ property to include the relevant options so that when the
		bonobo graph is serialized as a GraphViz document, different objects can be
		visually differentiated.
		'''
		super().__init__(*v, **kw)
		self.__name__ = f'{type(self).__name__} ({self.type})'

	def __call__(self, data):
		o = get_crom_object(data)
		if isinstance(o, self.type):
			yield data

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

class RecordCounter(Configurable):
	counts = Service('counts')
	verbose = Option(bool, default=False)
	name = Option()

	def __init__(self, *args, **kwargs):
		super().__init__(self, *args, **kwargs)
		self.mod = 100

	def __call__(self, data, counts):
		counts[self.name] += 1
		count = counts[self.name]
		if count % self.mod == 0:
			print(f'\r{count} {self.name}', end='', file=sys.stderr)
		return data

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

def deep_copy(data):
	# Not actually very deep, just shallow copies everything except
	# the object generated in another thread
	return {k:v for k,v in data.items() if k != "_LOD_OBJECT"}
