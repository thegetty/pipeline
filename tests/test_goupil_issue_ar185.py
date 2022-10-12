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
    classified_identifiers,
)
from cromulent import vocab

vocab.add_attribute_assignment_check()


class PIRModelingTest_AR185(TestGoupilPipelineOutput):
    def test_modeling_ar185(self):
        """
        AR-185 : Add modelling for Goupil Artists
        """
        output = self.run_pipeline("ar185")
        artists = output["model-person"]

        art1 = artists["tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,BONHEUR%2C%20ROSA"]
        art2 = artists["tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,LEMMENS%2C%20EMILE"]

        self.assertEqual(len(artists), 2)

        self.assertDictEqual(classification_tree(art1), {"French": {"Nationality": {}}})
        self.assertDictEqual(
            classified_identifiers(art1), {"Personal Name": "Rosa Bonheur", "Primary Name": "BONHEUR, ROSA"}
        )

        self.assertEqual(len(art1["exact_match"]), 1)  # has ulan link
        self.assertEqual(art1["exact_match"][0]["id"], "http://vocab.getty.edu/ulan/500014964")

        self.assertDictEqual(classification_tree(art2), {"French": {"Nationality": {}}})
        self.assertDictEqual(
            classified_identifiers(art2), {"Personal Name": "Lemmens", "Primary Name": "LEMMENS, EMILE"}
        )

        self.assertEqual(len(art2["exact_match"]), 1)  # has ulan link
        self.assertEqual(art2["exact_match"][0]["id"], "http://vocab.getty.edu/ulan/500099755")


if __name__ == "__main__":
    unittest.main()
