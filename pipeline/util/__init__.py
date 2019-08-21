import re
import os
import sys
import fnmatch
import pprint
import calendar
import datetime
from threading import Lock
from contextlib import ContextDecorator, suppress
from collections import defaultdict, namedtuple

import dateutil.parser
from bonobo.config import Configurable, Option, Service

import settings
import pipeline.io.arches
from cromulent import model
from cromulent.model import factory, BaseResource

# Dimension = namedtuple("Dimension", [
# 	'value',	# numeric value
# 	'unit',		# unit
# 	'which'		# e.g. width, height, ...
# ])
#
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

def implode_date(data: dict, prefix: str, clamp:str=None):
	'''
	Given a dict `data` and a string `prefix`, extract year, month, and day elements
	from `data` (e.g. '{prefix}year', '{prefix}month', and '{prefix}day'), and return
	an ISO 8601 date string ('YYYY-MM-DD'). If the day, or day and month elements are
	missing, may also return a year-month ('YYYY-MM') or year ('YYYY') string.

	If `clamp='begin'` and a year value is found, the resulting date string will use
	the earliest valid value for any field (month or day) that is not present or false.
	For example, '1800-02' would become '1800-02-01'.

	If `clamp='end'`, clamping occurs using the latest valid values. For example,
	'1800-02' would become '1800-02-28'.
	'''
	year = data.get(f'{prefix}year')
	try:
		year = int(year)
	except:
		return None
	month = data.get(f'{prefix}month', data.get(f'{prefix}mo'))
	day = data.get(f'{prefix}day')
	try:
		month = int(month)
		if month < 1 or month > 12:
			raise Exception
	except:
		if clamp == 'begin':
			month = 1
		elif clamp == 'end':
			month = 12

	try:
		day = int(day)
		if day < 1 or day > 31:
			raise Exception
	except:
		if clamp == 'begin':
			day = 1
		elif clamp == 'end':
			day = calendar.monthrange(year, month)[1]

	if year and month and day:
		return '%04d-%02d-%02d' % (int(year), month, day)
	elif year and month:
		return '%04d-%02d' % (int(year), month)
	elif year:
		return '%04d' % (int(year),)
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
			# NOTE: It's important that these attribute-based identity rules are
			#       based on crom classes that will not be top-level resources in Arches.
			#       That is, they must only be referenced from within a top-level
			#       resource, and not across resource boundaries. This is because during a
			#       merge, only one value will be presersved for non-multiple properties
			#       that differ between input objects such as `id` (and so anything
			#       referencing an `id` value that is dropped will be left with a dangling
			#       pointer).
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
			handled = False
			for attr, classes in self.attribute_based_identity.items():
				if isinstance(v, classes):
					if hasattr(v, 'content'):
						identified[getattr(v, attr)].append(v)
						handled = True
						break
			if not handled:
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
			if not obj.allows_multiple(p):
				setattr(obj, p, None)
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

class RecursiveExtractKeyedValue(ExtractKeyedValue):
	include_self = Option(bool, default=True)

	def __call__(self, data, *args, **kwargs):
		if self.include_self:
			a = data
		else:
			a = data.get(self.key)
		while a:
			child = {k: v for k, v in a.items()}
			child.update({
				'parent_data': data,
			})
			yield child
			data = a
			a = a.get(self.key)

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

def timespan_before(after):
	ts = model.TimeSpan()
	try:
		ts.end_of_the_end = after.begin_of_the_begin
		return ts
	except AttributeError:
		return None

def timespan_after(before):
	ts = model.TimeSpan()
	try:
		ts.begin_of_the_begin = before.end_of_the_end
		return ts
	except AttributeError:
		return None

def replace_key_pattern(pat, rep, value):
	r = re.compile(pat)
	d = {}
	for k, v in value.items():
		m = r.search(k)
		if m:
			d[k.replace(m.group(1), rep, 1)] = v
		else:
			d[k] = v
	return d

def strip_key_prefix(prefix, value):
	'''
	Strip the given `prefix` string from the beginning of all keys in the supplied `value`
	dict, returning a copy of `value` with the new keys.
	'''
	d = {}
	for k, v in value.items():
		if k.startswith(prefix):
			d[k.replace(prefix, '', 1)] = v
		else:
			d[k] = v
	return d

def timespan_from_outer_bounds(begin=None, end=None):
	'''
	Return a `TimeSpan` based on the (optional) `begin` and `end` date strings.

	If both `begin` and `end` are `None`, returns `None`.
	'''
	if begin or end:
		ts = model.TimeSpan(ident='')
		if begin is not None:
			try:
				if not isinstance(begin, datetime.datetime):
					begin = dateutil.parser.parse(begin)
				begin = begin.strftime("%Y-%m-%dT%H:%M:%SZ")
				ts.begin_of_the_begin = begin
			except ValueError:
				print(f'*** failed to parse begin date: {begin}')
				raise
		if end is not None:
			try:
				if not isinstance(end, datetime.datetime):
					end = dateutil.parser.parse(end)
				end = end.strftime("%Y-%m-%dT%H:%M:%SZ")
				ts.end_of_the_end = end
			except ValueError:
				print(f'*** failed to parse end date: {end}')
		return ts
	return None
