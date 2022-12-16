#!/usr/bin/env python3 -B
import unittest
import os
import os.path
import hashlib
import json
import uuid
import pprint

from tests import TestWriter, TestGoupilPipelineOutput, MODELS, classified_identifiers
from cromulent import vocab

vocab.add_attribute_assignment_check()


class PIRModelingTest_AR184(TestGoupilPipelineOutput):
    def test_modeling_ar184(self):
        """
        AR-184 : Add modelling for Goupil Organizations
        """
        output = self.run_pipeline("ar184")
        groups = output["model-groups"]

        self.assertEqual(len(groups), 4)
        org = groups["tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#ORG,ULAN,500125157"]

        self.assertEqual(len(org["exact_match"]), 1)  # has ulan link
        self.assertEqual(org["exact_match"][0]["id"], "http://vocab.getty.edu/ulan/500125157")
        self.assertEqual(
            classified_identifiers(org), {"Primary Name": "Metropolitan Museum of Art"}
        )  # has name and its primary

        self.assertEqual(
            org["referred_to_by"][0]["id"],
            "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Text,Book,6,Page,173,Row,8",
        )


if __name__ == "__main__":
    unittest.main()
