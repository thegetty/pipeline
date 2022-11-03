#!/usr/bin/env python3 -B
import unittest
import os
import os.path
import hashlib
import json
import uuid
import pprint

from tests import (
    TestWriter,
    TestGoupilPipelineOutput,
    MODELS,
    classified_identifiers,
    classification_tree,
    classification_sets,
)
from cromulent import vocab

vocab.add_attribute_assignment_check()


class PIRModelingTest_AR194(TestGoupilPipelineOutput):
    def test_modeling_ar194(self):
        """
        AR-194 : Add Provenance Invetorying modelling
        """
        output = self.run_pipeline("ar194")
        activities = output["model-activity"]

        inv = activities["tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#TX,Out,3,137,3"]
        maintenance = [p for p in inv["part"] if "Evaluated" in p["_label"]]

        self.assertEqual(len(maintenance), 1)
        maintenance = maintenance[0]
        contents = [note["content"] for note in maintenance["assigned"][0]["referred_to_by"]]
        self.assertIn("NXZ", contents)
        self.assertIn("Restauration", contents)
        self.assertIn("cadre", contents)
        self.assertIn("1000", contents)


if __name__ == "__main__":
    unittest.main()
