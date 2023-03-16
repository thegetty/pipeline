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
        import pdb; pdb.set_trace()
        
        obj1 = output["model-object"][
            "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Knoedler%27s"
        ]

        # if all activities have been merged successfully then in this case there's only one
        self.assertEqual(len(knoedler["carried_out"]), 1)
        # not that we are here also check the following are present
        self.assertIn("classified_as", knoedler["carried_out"][0])
        self.assertIn("referred_to_by", knoedler["carried_out"][0])
        self.assertIn("took_place_at", knoedler["carried_out"][0])


if __name__ == "__main__":
    unittest.main()
