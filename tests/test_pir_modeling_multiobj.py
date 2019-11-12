#!/usr/bin/env python3 -B
import unittest
import os
import os.path
import hashlib
import json
import uuid
import pprint
import inspect
from pathlib import Path
import warnings

from tests import TestProvenancePipelineOutput

class PIRModelingTest_MultiObject(TestProvenancePipelineOutput):
	def test_modeling_for_multi_object_lots(self):
		'''
		Test for modeling of lots containing multiple objects.
		'''
		output = self.run_pipeline('multiobj')
		warnings.warn(f'TODO: implement {inspect.getframeinfo(inspect.currentframe()).function}')


if __name__ == '__main__':
	unittest.main()
