import os
import sys
import csv
import fnmatch

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

	def read(self, path, *, fs):
		sys.stderr.write('============================== %s\n' % (path,))
		with fs.open(path, newline='') as csvfile:
			r = csv.reader(csvfile)
			for row in r:
				yield row

	__call__ = read