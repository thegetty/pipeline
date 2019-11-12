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

class PIRModelingTest_PrevSaleMerge(TestProvenancePipelineOutput):
	# TODO: add tests for prevsale_merge; this is different than the above tests,
	#       in that it requires post-processing that is normally carried out by the
	#       `rewrite_post_sales_uris.py` script.
	pass


if __name__ == '__main__':
	unittest.main()
