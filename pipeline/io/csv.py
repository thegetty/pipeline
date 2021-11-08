import os
import sys
import csv
import fnmatch
import warnings

from bonobo.constants import NOT_MODIFIED
from bonobo.nodes.io.file import FileReader
from bonobo.config import Configurable, Option, Service

class CurriedCSVReader(Configurable):
	'''
	This reader takes CSV filenames as input, and for each parses
	the CSV content and yields a tuple of strings for each row.
	'''
	fs = Service(
		'fs',
		__doc__='''The filesystem instance to use.''',
	)  # type: str
	mode = Option(
		str,
		default='r',
		__doc__='''What mode to use for open() call.''',
	)  # type: str
	encoding = Option(
		str,
		default='utf-8',
		__doc__='''Encoding.''',
	)  # type: str
	limit = Option(
		int,
		__doc__='''Limit the number of rows read (to allow early pipeline termination).''',
	)
	verbose = Option(
		bool,
		default=False
	)
	field_names = Option()

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.count = 0

	def read(self, path, *, fs):
		limit = self.limit
		count = self.count
		names = self.field_names
		error_emitted = False
		if not(limit) or (limit and count < limit):
			if self.verbose:
				sys.stderr.write('============================== %s\n' % (path,))
			with fs.open(path, newline='') as csvfile:
				r = csv.reader(csvfile)
				for line, row in enumerate(r):
					if not error_emitted:
						if len(row) != len(names):
							error_emitted = True
							warnings.warn(f'Column counts for header and content do not match ({len(names)} != {len(row)}) in {path}:{line+1}')
					if limit and count >= limit:
						break
					count += 1
					if names:
						d = {}
						for i in range(len(names)):
							d[names[i]] = row[i]
						yield d
					else:
						yield row
			self.count = count

	__call__ = read
