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

class PIRModelingTest_Objects(TestProvenancePipelineOutput):
	def test_modeling_for_objects(self):
		'''
		Test that all HumanMadeObjects are linked to a corresponding VisualItem.
		'''
		output = self.run_pipeline('objects')
		warnings.warn(f'TODO: implement {inspect.getframeinfo(inspect.currentframe()).function}')


if __name__ == '__main__':
	unittest.main()
