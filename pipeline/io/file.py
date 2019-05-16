import os
import os.path
import hashlib

from bonobo.config import Configurable, Option
from pipeline.util import ExclusiveValue

class FileWriter(Configurable):
	directory = Option(default="output")

	def __call__(self, data: dict):
		d = data['_OUTPUT']
		uuid = data['uuid']
		dr = os.path.join(self.directory, data['_ARCHES_MODEL'])
		with ExclusiveValue(dr):
			if not os.path.exists(dr):
				os.mkdir(dr)
		ddr = os.path.join(dr, uuid)
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
