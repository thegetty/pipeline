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

class PIRModelingTest_MultiLot(TestProvenancePipelineOutput):
	def test_modeling_for_multi_lot_procurements(self):
		'''
		Test for modeling of single procurements for which a single payment was made
		for multiple auction lots.
		'''
		output = self.run_pipeline('multilot')
		warnings.warn(f'TODO: implement {inspect.getframeinfo(inspect.currentframe()).function}')


if __name__ == '__main__':
	unittest.main()
