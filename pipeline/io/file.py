import os
import os.path
import hashlib
import uuid

from pipeline.util import CromObjectMerger

from bonobo.constants import NOT_MODIFIED
from bonobo.config import Configurable, Option
from pipeline.util import ExclusiveValue
from cromulent import model, reader
from cromulent.model import factory

def filename_for(data: dict):
	uu = data.get('uuid')
	if uu:
		partition = uu[:2]
	elif 'uri' in data:
		h = hashlib.md5(data['uri'].encode('utf-8')).hexdigest()
		partition = h[:2]
		uu = f'content-{h}'
# 		print(f'*** No UUID in top-level resource. Using a hash of top-level URI: {uu}')
	if not uu:
		uu = str(uuid.uuid4())
		partition = uu[:2]
# 		print(f'*** No UUID in top-level resource. Using an assigned UUID filename for the content: {uu}')
	fn = f'{uu}.json'
	return fn, partition

class FileWriter(Configurable):
	directory = Option(default="output")

	def __call__(self, data: dict):
		d = data['_OUTPUT']
		dr = os.path.join(self.directory, data['_ARCHES_MODEL'])
		with ExclusiveValue(dr):
			if not os.path.exists(dr):
				os.mkdir(dr)
		filename, partition = filename_for(data)
		fn = os.path.join(dr, filename)
		fh = open(fn, 'w')
		fh.write(d)
		fh.close()
		return data

class MultiFileWriter(Configurable):
	directory = Option(default="output")

	def __call__(self, data: dict):
		d = data['_OUTPUT']
		filename, partition = filename_for(data)
		dr = os.path.join(self.directory, data['_ARCHES_MODEL'])
		with ExclusiveValue(dr):
			if not os.path.exists(dr):
				os.mkdir(dr)
		ddr = os.path.join(dr, filename)
		with ExclusiveValue(ddr):
			if not os.path.exists(ddr):
				os.mkdir(ddr)
			h = hashlib.md5(d.encode('utf-8')).hexdigest()
			fn = os.path.join(ddr, "%s.json" % (h,))
			if not os.path.exists(fn):
				fh = open(fn, 'w')
				fh.write(d)
				fh.close()
			return data

class MergingFileWriter(Configurable):
	directory = Option(default="output")
	partition_directories = Option(default=False)
	compact = Option(default=True, required=False)
	model = Option(default=None, required=True)

	def __init__(self, *args, **kwargs):
		'''
		Sets the __name__ property to include the relevant options so that when the
		bonobo graph is serialized as a GraphViz document, different objects can be
		visually differentiated.
		'''
		super().__init__(self, *args, **kwargs)
		self.merger = CromObjectMerger()
		self.__name__ = f'{type(self).__name__} ({self.model})'

		self.dr = os.path.join(self.directory, self.model)
		with ExclusiveValue(self.dr):
			if not os.path.exists(self.dr):
				os.mkdir(self.dr)
			if self.partition_directories:
				for partition in [('00' + str(hex(x))[2:])[-2:] for x in range(256)]:
					pp = os.path.join(self.dr, partition)
					if not os.path.exists(pp):
						os.mkdir(pp)

	def merge(self, model_object, fn):
		r = reader.Reader()
		merger = self.merger
		with open(fn, 'r') as fh:
			content = fh.read()
			try:
				m = r.read(content)
				if m == model_object:
					return None
				else:
					merger.merge(m, model_object)
					return m
			except model.DataError as e:
				print(f'Exception caught while merging data from {fn} ({str(e)}):')
				print(factory.toString(model_object, False))
				print(content)
				raise
		
	def __call__(self, data: dict):
		filename, partition = filename_for(data)
		factory = data['_CROM_FACTORY']
		model_object = data['_LOD_OBJECT']

		dr = self.dr
		if self.partition_directories:
			dr = os.path.join(dr, partition)
		
		with ExclusiveValue(dr):
			fn = os.path.join(dr, filename)
			if os.path.exists(fn):
				m = self.merge(model_object, fn)
				if m:
					d = factory.toString(m, self.compact)
				else:
					d = None
			else:
				d = factory.toString(model_object, self.compact)

			if d:
				with open(fn, 'w', encoding='utf-8') as fh:
					fh.write(d)
			return NOT_MODIFIED
