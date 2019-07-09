import os
import sys
import fnmatch
from threading import Lock
from contextlib import ContextDecorator, suppress
from collections import defaultdict, namedtuple

import settings
import pipeline.io.arches
from bonobo.config import Configurable, Option, Service
from cromulent import model
from cromulent.model import factory, BaseResource

Dimension = namedtuple("Dimension", [
	'value',	# numeric value
	'unit',		# unit
	'which'		# e.g. width, height, ...
])

def identity(d):
	'''
	Simply yield the value that is passed as an argument.

	This is trivial, but necessary for use in constructing some bonobo graphs.
	For example, if two already instantiated graph chains need to be connected,
	one being used as input to the other, bonobo does not allow this:

	`graph.add_chain(_input=prefix.output, _output=suffix.input)`

	Instead, the `add_chain` call requires at least one graph node to be added. Hence:

	`graph.add_chain(identity, _input=prefix.output, _output=suffix.input)`
	'''
	yield d

def implode_date(data: dict, prefix: str):
	'''
	Given a dict `data` and a string `prefix`, extract year, month, and day elements
	from `data` (e.g. '{prefix}year', '{prefix}month', and '{prefix}day'), and return
	an ISO 8601 date string ('YYYY-MM-DD'). If the day, or day and month elements are
	missing, may also return a year-month ('YYYY-MM') or year ('YYYY') string.
	'''
	year = data.get(f'{prefix}year')
	month = data.get(f'{prefix}month', data.get(f'{prefix}mo'))
	day = data.get(f'{prefix}day')
	if year and month and day:
		return f'{year}-{month}-{day}'
	elif year and month:
		return f'{year}-{month}'
	elif year:
		return f'{year}'
	return None

class ExclusiveValue(ContextDecorator):
	_locks = {}
	lock = Lock()

	def __init__(self, wrapped):
		self._wrapped = wrapped

	def get_lock(self):
		_id = self._wrapped
		with ExclusiveValue.lock:
			if not _id in ExclusiveValue._locks:
				ExclusiveValue._locks[_id] = Lock()
		return ExclusiveValue._locks[_id]

	def __enter__(self):
		self.get_lock().acquire()
		return self._wrapped

	def __exit__(self, *exc):
		self.get_lock().release()

def configured_arches_writer():
	return pipeline.io.arches.ArchesWriter(
		endpoint=settings.arches_endpoint,
		auth_endpoint=settings.arches_auth_endpoint,
		username=settings.arches_endpoint_username,
		password=settings.arches_endpoint_password,
		client_id=settings.arches_client_id
	)

class CromObjectMerger:
	def __init__(self):
		self.attribute_based_identity = {
			'content': (model.Name, model.Identifier),
		}

	def merge(self, obj, *to_merge):
		# print('merging...')
		# print(f'base object: {obj}')
		for m in to_merge:
			# print('============================================')
			# print(f'merge: {m}')
			for p in m.list_my_props():
				value = None
				with suppress(AttributeError):
					value = getattr(m, p)
				if value is not None:
# 					print(f'{p}: {value}')
					if isinstance(value, list):
						self.set_or_merge(obj, p, *value)
					else:
						self.set_or_merge(obj, p, value)
# 			obj = self.merge(obj, m)
# 		print('Result of merge:')
# 		print(factory.toString(obj, False))
		return obj

	def set_or_merge(self, obj, p, *values):
		existing = []
# 		print('------------------------')
		with suppress(AttributeError):
			e = getattr(obj, p)
			if isinstance(e, list):
				existing.extend(e)
			else:
				existing = [e]

# 		print(f'Setting {p}')
		identified = defaultdict(list)
		unidentified = []
		for v in existing + list(values):
			for attr, classes in self.attribute_based_identity.items():
				if isinstance(v, classes):
					if hasattr(v, 'content'):
						identified[getattr(v, attr)].append(v)
					continue
			if hasattr(v, 'id'):
				identified[v.id].append(v)
			else:
				unidentified.append(v)
# 			print(f'- {v}')

		if p == 'type':
			# print('*** TODO: calling setattr(_, "type") on crom objects throws; skipping')
			return
		setattr(obj, p, None)
		for v in identified.values():
			if not obj.allows_multiple(p):
				setattr(obj, p, None)
			setattr(obj, p, self.merge(*v))
		for v in unidentified:
			setattr(obj, p, v)

class ExtractKeyedValues(Configurable):
	'''
	Given a `dict` representing an some object, extract an array of `dict` values from
	the `key` member. To each of the extracted dictionaries, add a 'parent_data' key with
	the value of the original dictionary. Yield each extracted dictionary.
	'''
	key = Option(str, required=True)
	include_parent = Option(bool, default=True)

	def __init__(self, *v, **kw):
		'''
		Sets the __name__ property to include the relevant options so that when the
		bonobo graph is serialized as a GraphViz document, different objects can be
		visually differentiated.
		'''
		super().__init__(*v, **kw)
		self.__name__ = f'{type(self).__name__} ({self.key})'

	def __call__(self, data, *args, **kwargs):
		for a in data.get(self.key, []):
			child = {k: v for k, v in a.items()}
			child.update({
				'parent_data': data,
			})
			yield child

class ExtractKeyedValue(Configurable):
	'''
	Given a `dict` representing an some object, extract the `key` member (a dict).
	To the extracted dictionaries, add a 'parent_data' key with
	the value of the original dictionary. Yield the extracted dictionary.
	'''
	key = Option(str, required=True)
	include_parent = Option(bool, default=True)

	def __init__(self, *v, **kw):
		'''
		Sets the __name__ property to include the relevant options so that when the
		bonobo graph is serialized as a GraphViz document, different objects can be
		visually differentiated.
		'''
		super().__init__(*v, **kw)
		self.__name__ = f'{type(self).__name__} ({self.key})'

	def __call__(self, data, *args, **kwargs):
		a = data.get(self.key)
		if a:
			child = {k: v for k, v in a.items()}
			child.update({
				'parent_data': data,
			})
			yield child

class MatchingFiles(Configurable):
	'''
	Given a path and a pattern, yield the names of all files in the path that match the pattern.
	'''
	path = Option(str)
	pattern = Option(str, default='*')
	fs = Service(
		'fs',
		__doc__='''The filesystem instance to use.''',
	)  # type: str

	def __init__(self, *args, **kwargs):
		super().__init__(self, *args, **kwargs)
		self.__name__ = f'{type(self).__name__} ({self.pattern})'

	def __call__(self, *, fs, **kwargs):
		count = 0
		subpath, pattern = os.path.split(self.pattern)
		fullpath = os.path.join(self.path, subpath)
		for f in sorted(fs.listdir(fullpath)):
			if fnmatch.fnmatch(f, pattern):
				yield os.path.join(subpath, f)
				count += 1
		if not count:
			sys.stderr.write(f'*** No files matching {pattern} found in {fullpath}\n')
