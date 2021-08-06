import sys
import lxml.etree

from bonobo.constants import NOT_MODIFIED
from bonobo.nodes.io.file import FileReader
from bonobo.config import Configurable, Option, Service

class XMLReader(FileReader):
	'''
	A FileReader that parses an XML file and yields lxml.etree Element objects matching
	the given XPath expression.
	'''
	xpath = Option(str, required=True)

	def read(self, file):
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
	limit = Option(
		int,
		__doc__='''Limit the number of rows read (to allow early pipeline termination).''',
	)
	verbose = Option(
		bool,
		default=False
	)

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.count = 0

	def read(self, path, *, fs):
		limit = self.limit
		count = self.count
		if not(limit) or (limit and count < limit):
			if self.verbose:
				sys.stderr.write('============================== %s\n' % (path,))
			file = fs.open(path, self.mode, encoding=self.encoding)
			root = lxml.etree.parse(file)
			for e in root.xpath(self.xpath):
				if limit and count >= limit:
					break
				count += 1
				yield e
			self.count = count
			file.close()

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
		return None

def print_xml_element(e):
	s = lxml.etree.tostring(e).decode('utf-8')
	print(s.replace('\n', ' '))
	return NOT_MODIFIED

def print_xml_element_text(e):
	print(e.text)
	return NOT_MODIFIED
