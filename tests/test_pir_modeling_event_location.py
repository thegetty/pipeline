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

class PIRModelingTest_EventLocation(TestProvenancePipelineOutput):
	def test_modeling_for_event_locations(self):
		'''
		Test for modeling of the locations of auction events, including a Place hierarchy.
		'''
		output = self.run_pipeline('event_location')
		warnings.warn(f'TODO: implement {inspect.getframeinfo(inspect.currentframe()).function}')


if __name__ == '__main__':
	unittest.main()
