#!/usr/bin/env python3 -B
import unittest
import os.path

from tests import TestGoupilPipelineOutput, classified_identifiers, classification_sets
from cromulent import vocab

vocab.add_attribute_assignment_check()


class PIRModelingTest_AR212(TestGoupilPipelineOutput):
    def test_modeling_ar212(self):
        """
        AR-212 : Assert Dimensions have row references
        """
        output = self.run_pipeline("ar212")


if __name__ == "__main__":
    unittest.main()
