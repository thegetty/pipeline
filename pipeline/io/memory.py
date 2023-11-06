import os
import os.path
import hashlib
import uuid
import pprint
import traceback
import warnings
from collections import Counter, defaultdict, namedtuple
# import multiprocessing
# from multiprocessing.pool import ThreadPool

from pipeline.util import CromObjectMerger

from bonobo.constants import NOT_MODIFIED
from bonobo.config import Configurable, Option
from pipeline.util import ExclusiveValue
from cromulent import model, reader
from cromulent.model import factory
from .file import MergingFileWriter
from pipeline.linkedart import add_crom_data, get_crom_object

class MergingMemoryWriter(Configurable):
	directory = Option(default="output")
	partition_directories = Option(default=False)
	compact = Option(default=True, required=False)
	model = Option(default=None, required=True)
	limit = Option(default=None, required=False)

	def __init__(self, *args, **kwargs):
		'''
		Sets the __name__ property to include the relevant options so that when the
		bonobo graph is serialized as a GraphViz document, different objects can be
		visually differentiated.
		'''
		super().__init__(self, *args, **kwargs)
		self.data = {}
		self.counter = Counter()
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
		except Exception as e:
			print(f'Exception caught while merging data ({e}):')
			print(factory.toString(m, False))
			print(factory.toString(model_object, False))
			raise

	def __call__(self, data: dict):
		# import pdb; pdb.set_trace()
		# check what is going on with LOD_OBJECT
		if '_LOD_OBJECT' in data:
			# with open('no_LOD.txt', 'a') as f:
			# 	f.write(str(data))
			# 	f.write('\n')
		
			model_object = data['_LOD_OBJECT']
			ident = model_object.id
			self.counter['total'] += 1
			if ident in self.data:
				self.counter['collision'] += 1
				self.data[ident] = self.merge(model_object)
			else:
				self.counter['non-collision'] += 1
				self.data[ident] = model_object

			if self.limit is not None and len(self.data) >= self.limit:
				self.flush(verbose=False)

		return None

	def flush(self, verbose=True):
		writer = MergingFileWriter(directory=self.directory, partition_directories=self.partition_directories, compact=self.compact, model=self.model)
		count = len(self.data)
		skip = max(int(count / 100), 1)
		for i, k in enumerate(sorted(self.data)):
			o = self.data[k]
			if (i % skip) == 0:
				pct = 100.0 * float(i) / float(count)
				if verbose:
					print('[%d/%d] %.1f%% writing objects for model %s' % (i+1, count, pct, self.model))
			d = add_crom_data(data={}, what=o)
			try:
				writer(d)
			except:
				traceback.print_exc()
				continue
		if verbose:
			warnings.warn(f'MergingMemoryWriter flush for model {self.model} with {len(self.data)} items')
		self.data = {}
