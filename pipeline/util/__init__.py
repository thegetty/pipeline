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
import warnings

import dateutil.parser
from bonobo.config import Configurable, Option, Service

import settings
import pipeline.io.arches
from cromulent import model, vocab
from cromulent.model import factory, BaseResource
from pipeline.linkedart import add_crom_data

UNKNOWN_DIMENSION = 'http://vocab.getty.edu/aat/300055642'

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

def implode_date(data:dict, prefix:str='', clamp:str=None):
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

	If `clamp='eoe'` ('end of the end'), clamping occurs using the first value that is
	*not* valid. That is, the returned value may be used as an exclusive endpoint for a
	date range. For example, '1800-02' would become '1800-03-01'.
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
			raise ValueError(f'Month value is not valid: {month}')
	except Exception as e:
		if clamp == 'begin':
			month = 1
			day = 1
			return '%04d-%02d-%02d' % (int(year), month, day)
		elif clamp == 'end':
			day = 31
			month = 12
			return '%04d-%02d-%02d' % (int(year), month, day)
		elif clamp == 'eoe':
			day = 1
			month = 1
			year += 1
			return '%04d-%02d-%02d' % (int(year), month, day)
		else:
			return '%04d' % (int(year),)

	max_day = calendar.monthrange(year, month)[1]
	try:
		day = int(day)
		if day < 1 or day > 31:
			raise ValueError(f'Day value is not valid: {day}')
		if clamp == 'eoe':
			day += 1
			if day > max_day:
				day = 1
				month += 1
				if month > 12:
					month = 1
					year += 1
	except Exception as e:
		if clamp == 'begin':
			day = 1
		elif clamp == 'end':
			day = max_day
		elif clamp == 'eoe':
			day = 1
			month += 1
			if month > 12:
				month = 1
				year += 1
		else:
			if type(e) not in (TypeError, ValueError):
				warnings.warn(f'Failed to interpret day value {day!r} in implode_date: {e}')
				pprint.pprint(data)

	try:
		if year and month and day:
			return '%04d-%02d-%02d' % (int(year), month, day)
		elif year and month:
			return '%04d-%02d' % (int(year), month)
		elif year:
			return '%04d' % (int(year),)
	except TypeError as e:
		warnings.warn(f'*** {e}: {pprint.pformat([int(year), month, day])}')
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
			'value': (model.Dimension,),
		}
		self.metatyped_attribute_based_identity = {
			# This is similar to `self.attribute_based_identity`, but instead of being
			# based on the `type` of the object, it is based on the meta-type value
			# (the `obj.classified_as.classified_as` value)
			# of the object
			'content': (vocab._BriefText,),
		}
		
		# instead of mapping to a tuple of classes, `self._metatyped_attribute_based_identity`
		# maps to a list of sets of URIs (the set of classifications that must be present to be
		# interpreted as a member of the class)
		self._metatyped_attribute_based_identity = {}
		for attr, classes in self.metatyped_attribute_based_identity.items():
			id_sets = []
			for c in classes:
				o = c()
				ids = {mt.id for cl in o.classified_as for mt in getattr(cl, 'classified_as', [])}
				id_sets.append(ids)
			self._metatyped_attribute_based_identity[attr] = id_sets

	def merge(self, obj, *to_merge):
		if not to_merge:
			return obj
# 		print(f'merge called with {1+len(to_merge)} objects: ({obj}, {to_merge})')
		for m in to_merge:
			if obj == m:
				continue
			for p in m.list_my_props():
				try:
					value = getattr(m, p)
					if value is not None:
						if isinstance(value, list):
							self.set_or_merge(obj, p, *value)
						else:
							self.set_or_merge(obj, p, value)
				except AttributeError:
					pass
		return obj

	def _classify_values(self, values, identified, unidentified):
		for v in values:
			handled = False
			for attr, classes in self.attribute_based_identity.items():
				if isinstance(v, classes) and hasattr(v, attr):
					identified[getattr(v, attr)].append(v)
					handled = True
					break
			for attr, id_sets in self._metatyped_attribute_based_identity.items():
				if handled:
					break
				if hasattr(v, 'classified_as') and hasattr(v, attr):
					obj_ids = {mt.id for cl in v.classified_as for mt in getattr(cl, 'classified_as', [])}
					for id_set in id_sets:
						if id_set <= obj_ids:
							identified[getattr(v, attr)].append(v)
							handled = True
							break
			if not handled:
				try:
					i = v.id
					if i:
						identified[i].append(v)
					else:
						unidentified.append(v)
				except AttributeError:
					unidentified.append(v)
		if len(identified) > 1 and UNKNOWN_DIMENSION in identified:
			# drop the Unknown physical dimension (300055642)
			del(identified[UNKNOWN_DIMENSION])

	def set_or_merge(self, obj, p, *values):
		if p == 'type':
			# print('*** TODO: calling setattr(_, "type") on crom objects throws; skipping')
			return

		existing = []
		try:
			e = getattr(obj, p)
			if isinstance(e, list):
				existing = e
			else:
				existing = [e]
		except AttributeError:
			pass

		identified = defaultdict(list)
		unidentified = []
		self._classify_values(values, identified, unidentified)

		allows_multiple = obj.allows_multiple(p)
		if identified:
			# there are values in the new objects that have to be merged with existing identifiable values
			self._classify_values(existing, identified, unidentified)

			setattr(obj, p, None) # clear out all the existing values
			if allows_multiple:
				for _, v in sorted(identified.items()):
					setattr(obj, p, self.merge(*v))
				for v in unidentified:
					setattr(obj, p, v)
			else:
				try:
					identified_values = sorted(identified.values())[0]
				except TypeError:
					# in case the values cannot be sorted
					identified_values = list(identified.values())[0]
				setattr(obj, p, self.merge(*identified_values))

				if unidentified:
					warnings.warn(f'*** Dropping {len(unidentified)} unidentified values for property {p} of {obj}')
# 					unidentified_value = sorted(unidentified)[0]
# 					setattr(obj, p, unidentified_value)
		else:
			# there are no identifiable values in the new objects, so we can just append them
			if allows_multiple:
				for v in unidentified:
					setattr(obj, p, v)
			else:
				if unidentified:
					if len(unidentified) > 1:
						warnings.warn(f'*** Dropping {len(unidentified)-1} extra unidentified values for property {p} of {obj}')
					try:
						if hasattr(obj, p):
							values = set(unidentified + [getattr(obj, p)])
						else:
							values = set(unidentified)
						value = sorted(values)[0]
					except TypeError:
						# in case the values cannot be sorted
						value = unidentified[0]
					setattr(obj, p, None)
					setattr(obj, p, value)

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
	pattern = Option(default='*')
	fs = Service(
		'fs',
		__doc__='''The filesystem instance to use.''',
	)  # type: str

	def __init__(self, *args, **kwargs):
		'''
		Sets the __name__ property to include the relevant options so that when the
		bonobo graph is serialized as a GraphViz document, different objects can be
		visually differentiated.
		'''
		super().__init__(self, *args, **kwargs)
		self.__name__ = f'{type(self).__name__} ({self.pattern})'

	def __call__(self, *, fs, **kwargs):
		count = 0
		if not self.pattern:
			return
# 		print(repr(self.pattern))
		subpath, pattern = os.path.split(self.pattern)
		fullpath = os.path.join(self.path, subpath)
		for f in sorted(fs.listdir(fullpath)):
			if fnmatch.fnmatch(f, pattern):
				yield os.path.join(subpath, f)
				count += 1
		if not count:
			sys.stderr.write(f'*** No files matching {pattern} found in {fullpath}\n')

def make_ordinal(n):
	n = int(n)
	suffix = ['th', 'st', 'nd', 'rd', 'th'][min(n % 10, 4)]
	if 11 <= (n % 100) <= 13:
		suffix = 'th'
	return f'{n}{suffix}'

def timespan_for_century(century, narrow=False):
	'''
	Given a integer representing a century (e.g. 17 for the 17th century), return a
	TimeSpan object for the bounds of that century.
	
	If `narrow` is True, the bounding properties will be `end_of_the_begin` and
	`begin_of_the_end`; otherwise they will be `begin_of_the_begin` and `end_of_the_end`.
	'''
	ord = make_ordinal(century)
	ts = model.TimeSpan(ident='', label=f'{ord} century')
	from_year = 100 * (century-1)
	to_year = from_year + 100
	if narrow:
		ts.end_of_the_begin = "%04d-%02d-%02dT%02d:%02d:%02dZ" % (from_year, 1, 1, 0, 0, 0)
		ts.begin_of_the_end = "%04d-%02d-%02dT%02d:%02d:%02dZ" % (to_year, 1, 1, 0, 0, 0)
	else:
		ts.begin_of_the_begin = "%04d-%02d-%02dT%02d:%02d:%02dZ" % (from_year, 1, 1, 0, 0, 0)
		ts.end_of_the_end = "%04d-%02d-%02dT%02d:%02d:%02dZ" % (to_year, 1, 1, 0, 0, 0)
	return ts

def dates_for_century(century):
	'''
	Given a integer representing a century (e.g. 17 for the 17th century), return a
	tuple of dates for the bounds of that century.
	'''
	ord = make_ordinal(century)
	ts = model.TimeSpan(ident='', label=f'{ord} century')
	from_year = 100 * (century-1)
	to_year = from_year + 100
	begin = datetime.datetime(from_year, 1, 1)
	end = datetime.datetime(to_year, 1, 1)
	return (begin, end)

def timespan_before(after):
	ts = model.TimeSpan(ident='')
	try:
		ts.end_of_the_end = after.begin_of_the_begin
		with suppress(AttributeError):
			l = f'Before {after._label}'
			l.identified_by = model.Name(ident='', content=l)
			ts._label = l
		return ts
	except AttributeError:
		return None

def timespan_after(before):
	ts = model.TimeSpan(ident='')
	try:
		ts.begin_of_the_begin = before.end_of_the_end
		with suppress(AttributeError):
			l = f'After {before._label}'
			l.identified_by = model.Name(ident='', content=l)
			ts._label = l
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


def label_for_timespan_range(begin, end, inclusive=False):
	'''
	Returns a human-readable string for labeling the timespan with the given bounds.
	
	The {inclusive} indicates if the upper bound given by {end} is inclusive or exclusive.
	
	If {end} is exclusive, the label will take this into account in creating a
	human-readable string. For example, if the upper bound was '2019-12-01', exclusive,
	the human-readable label should indicate the timespan ending at the end of November.
	'''
	if begin == end:
		return begin
	
	if isinstance(begin, datetime.datetime):
		begin = begin.strftime("%Y-%m-%d")
	if isinstance(end, datetime.datetime):
		end = end.strftime("%Y-%m-%d")


	orig_begin = begin
	orig_end = end
	if begin.count('-') != 2:
		if not inclusive:
			raise Exception(f'truncated date strings must operate in inclusive mode in label_for_timespan_range: {begin}')
		begin = implode_date(dict(zip(('year', 'month', 'day'), (begin.split('-', 3) + ['', '', ''])[:3])), clamp='begin')
	if end.count('-') != 2:
		if not inclusive:
			raise Exception(f'truncated date strings must operate in inclusive mode in label_for_timespan_range: {end}')
		end = implode_date(dict(zip(('year', 'month', 'day'), (end.split('-', 3) + ['', '', ''])[:3])), clamp='end' if inclusive else 'eoe')
	
	beginparts = list(map(int, begin.split('-')))
	endparts = list(map(int, end.split('-')))

	from_y, from_m, from_d = beginparts
	to_y, to_m, to_d = endparts
	if inclusive:
		maxday = calendar.monthrange(to_y, to_m)[1]
		if from_y == to_y and from_m == to_m and from_d == 1 and to_d == maxday:
			# 1 month range
			return '%04d-%02d' % (from_y, from_m)
		elif from_y == to_y and from_m == 1 and to_m == 12 and from_d == 1 and to_d == 31:
			# 1 year range
			return str(from_y)
		else:
			return f'{orig_begin} to {orig_end}'
	else:
		if from_y == to_y and from_m == to_m and from_d == to_d - 1:
			# 1 day range
			return begin
		elif from_y == to_y and from_m == to_m - 1 and from_d == to_d and to_d == 1:
			# 1 month range
			return '%04d-%02d' % (from_y, from_m)
		elif from_y == to_y - 1 and from_m == to_m and to_m == 1 and from_d == to_d and to_d == 1:
			# 1 year range
			return str(from_y)
		else:
			to_d -= 1
			if to_d == 0:
				to_m -= 1
				if to_m == 0:
					to_m = 12
					to_y -= 1
				to_d = calendar.monthrange(to_y, to_m)[1]
			end = '%04d-%02d-%02d' % (to_y, to_m, to_d)
			return f'{begin} to {end}'


def timespan_from_outer_bounds(begin=None, end=None, inclusive=False):
	'''
	Return a `TimeSpan` based on the (optional) `begin` and `end` date strings.

	If both `begin` and `end` are `None`, returns `None`.
	'''
	if begin or end:
		ts = model.TimeSpan(ident='')
		if begin and end:
			ts._label = label_for_timespan_range(begin, end, inclusive=inclusive)
		elif begin:
			ts._label = f'{begin} onwards'
		elif end:
			ts._label = f'up to {end}'

		if begin is not None:
			try:
				if not isinstance(begin, datetime.datetime):
					begin = dateutil.parser.parse(begin)
				begin = begin.strftime("%Y-%m-%dT%H:%M:%SZ")
				ts.begin_of_the_begin = begin
			except ValueError:
				warnings.warn(f'*** failed to parse begin date: {begin}')
				raise
		if end is not None:
			try:
				if not isinstance(end, datetime.datetime):
					end = dateutil.parser.parse(end)
				if inclusive:
					end += datetime.timedelta(days=1)
				end = end.strftime("%Y-%m-%dT%H:%M:%SZ")
				ts.end_of_the_end = end
			except ValueError:
				warnings.warn(f'*** failed to parse end date: {end}')
		return ts
	return None

class CaseFoldingSet(set):
	def __init__(self, iterable):
		super().__init__(self)
		for v in iterable:
			self.add(v)

	def __and__(self, value):
		return CaseFoldingSet({s for s in value if s in self})

	def __or__(self, value):
		s = CaseFoldingSet({})
		for v in self:
			super().add(v)
		for v in value:
			s.add(v)
		return s

	def add(self, v):
		super().add(v.casefold())

	def remove(self, v):
		super().remove(v.casefold())

	def __contains__(self, v):
		return super().__contains__(v.casefold())

	def intersects(self, values):
		if isinstance(values, CaseFoldingSet):
			l = set(self)
			r = set(values)
			return l & r
		else:
			for v in values:
				if v in self:
					return True
			return False

def truncate_with_ellipsis(s, length=100):
	'''
	If the string is too long to represent as a title-like identifier, return a new,
	truncated string with a trailing ellipsis that can be used as a title (with the
	assumption that the long original value will be represented as a more suitable
	string such as a description).
	'''
	if not isinstance(s, str):
		return None
	if len(s) <= length:
		return None
	shorter = ' '.join(s[:length].split(' ')[0:-1]) + '…'
	if len(shorter) == 1:
		# breaking on spaces did not yield a shorter string;
		# {s} must start with at least 100 non-space characters
		shorter = s[:length-1] + '…'
	return shorter

class GraphListSource:
	'''
	Act as a bonobo graph source node for a set of crom objects.
	Yields the supplied objects wrapped in data dicts.
	'''
	def __init__(self, values, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.values = values

	def __call__(self):
		for v in self.values:
			yield add_crom_data({}, v)

def rename_keys(mapping:dict):
	return lambda d, p: {mapping[k] if k in mapping else k: v for k, v in d.items()}

def _as_list(data):
	if isinstance(data, list):
		return data
	elif data is None:
		return []
	else:
		return [data]
