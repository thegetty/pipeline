import os
import fnmatch
from threading import Lock
from contextlib import ContextDecorator, suppress
from collections import defaultdict

import settings
import pipeline.io.arches
from bonobo.config import Configurable, Option, Service
from cromulent.model import factory

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
	def merge(self, obj, *to_merge):
		print('merging...')
		propInfo = obj._list_all_props()
# 		print(f'base object: {obj}')
		for m in to_merge:
			pass
# 			print('============================================')
# 			print(f'merge: {m}')
			for p in propInfo.keys():
				value = None
				with suppress(AttributeError):
					value = getattr(m, p)
				if value is not None:
# 					print(f'{p}: {value}')
					if type(value) == list:
						self.set_or_merge(obj, p, *value)
					else:
						self.set_or_merge(obj, p, value)
# 			obj = self.merge(obj, m)
		print('Result of merge:')
		print(factory.toString(obj, False))
		return obj

	def set_or_merge(self, obj, p, *values):
		print('------------------------')
		existing = []
		with suppress(AttributeError):
			existing = getattr(obj, p)
			if type(existing) == list:
				existing.extend(existing)
			else:
				existing = [existing]

		print(f'Setting {p}')
		identified = defaultdict(list)
		unidentified = []
		if existing:
			print('Existing value(s):')
			for v in existing:
				if hasattr(v, 'id'):
					identified[v.id].append(v)
				else:
					unidentified.append(v)
				print(f'- {v}')

		for v in values:
			print(f'Setting {p} value to {v}')
			if hasattr(v, 'id'):
				identified[v.id].append(v)
			else:
				unidentified.append(v)

		if p == 'type':
			print('*** TODO: calling setattr(_, "type") on crom objects throws an exception; skipping for now')
			return
		for i, v in identified.items():
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

	def __call__(self, data):
		for a in data.get(self.key, []):
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

