from bonobo.constants import NOT_MODIFIED
from bonobo.nodes.io.file import FileReader
from bonobo.config import Configurable, Method, Option
import lxml.etree

class XMLReader(FileReader):
	xpath = Option(str, required=True)

	@Method(positional=False)
	def loader(self, file):
		root = lxml.etree.parse(filename)
		return root

	def read(self, file, *, fs):
		root = self.loader(file.read())
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
