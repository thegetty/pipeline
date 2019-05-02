import os
import sys
import lxml.etree
import fnmatch

from bonobo.constants import NOT_MODIFIED
from bonobo.nodes.io.file import FileReader
from bonobo.config import Configurable, Method, Option, Service

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
		for f in fs.listdir(fullpath):
			if fnmatch.fnmatch(f, pattern):
				yield os.path.join(subpath, f)
				count += 1
		if not count:
			sys.stderr.write(f'*** No files matching {pattern} found in {fullpath}\n')

class XMLReader(FileReader):
	'''
	A FileReader that parses an XML file and yields lxml.etree Element objects matching
	the given XPath expression.
	'''
	xpath = Option(str, required=True)

	def read(self, file, *, fs):
		root = lxml.etree.parse(file)
		for e in root.xpath(self.xpath):
			yield e

	__call__ = read

class CurriedXMLReader(Configurable):
	'''
	Similar to XMLReader, this reader takes XML filenames as input, and for each parses
	the XML content and yields lxml.etree Element objects matching the given XPath
	expression.
	'''
	xpath = Option(str, required=True)
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
		file = fs.open(path, self.mode, encoding=self.encoding)
		root = lxml.etree.parse(file)
		for e in root.xpath(self.xpath):
			yield e

	__call__ = read

class ExtractXPath(Configurable):
	xpath = Option(str, required=True)

	def __call__(self, e):
		for a in e.xpath(self.xpath):
			yield a

class FilterXPathEqual(Configurable):
	xpath = Option(str, required=True)
	value = Option(str)

	def __call__(self, e):
		for t in e.xpath(self.xpath):
			if t.text == self.value:
				return NOT_MODIFIED

def print_xml_element(e):
	s = lxml.etree.tostring(e).decode('utf-8')
	print(s.replace('\n', ' '))
	return NOT_MODIFIED

def print_xml_element_text(e):
	print(e.text)
	return NOT_MODIFIED
