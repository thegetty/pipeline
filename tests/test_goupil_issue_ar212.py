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
        # TODO the following test cases:
        # 
        # the one assigned by Goupil should be 'Object Stock Number' http://vocab.getty.edu/aat/300412177 
        # the one assigned by GPI should be 'STAR Identifier' https://data.getty.edu/local/thesaurus/star-identifier
        # retouch par and or modifiers
        # dimension source ref pointing to row
        self.assertTrue(False)

if __name__ == "__main__":
    unittest.main()
