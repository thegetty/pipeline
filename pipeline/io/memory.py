import os
import os.path
import hashlib
import uuid

from pipeline.util import CromObjectMerger

from bonobo.constants import NOT_MODIFIED
from bonobo.config import Configurable, Option
from pipeline.util import ExclusiveValue
from cromulent import model, reader
from .file import MergingFileWriter
from pipeline.linkedart import add_crom_data, get_crom_object

class MergingMemoryWriter(Configurable):
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
		self.data = {}
		self.merger = CromObjectMerger()
		self.__name__ = f'{type(self).__name__} ({self.model})'

	def merge(self, model_object):
		merger = self.merger
		ident = model_object.id
		try:
			m = self.data.get(ident)
			if not m:
				return model_object
			if m == model_object:
				return model_object
			else:
				merger.merge(m, model_object)
				return m
		except model.DataError:
			print(f'Exception caught while merging data:')
			print(d)
			print(content)
			raise

	def flush(self):
		writer = MergingFileWriter(directory=self.directory, partition_directories=self.partition_directories, compact=self.compact, model=self.model)
		objects = self.data.values()
		count = len(objects)
		skip = max(count / 400, 1)
		for i, o in enumerate(objects):
			if (i % skip) == 0:
				pct = 100.0 * float(i) / float(count)
				print('[%d/%d] %.1f%% writing objects for model %s' % (i, count, pct, self.model))
			d = add_crom_data(data={}, what=o)
			writer(d)
		print(f'100.0% writing objects for model {self.model}')

	def __call__(self, data: dict):
		model_object = data['_LOD_OBJECT']
		ident = model_object.id
		if ident in self.data:
			self.data[ident] = self.merge(model_object)
		else:
			self.data[ident] = model_object
		return NOT_MODIFIED
