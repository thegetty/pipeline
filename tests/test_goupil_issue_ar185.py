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

    def test_modeling_ar185_2(self):
        """
        AR-185 : Add modelling for Sellers and Buyers
        """
        output = self.run_pipeline("ar185")
        people = output["model-person"]

        seller1 = people["tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#PERSON,PI,G-42810,person-0"]
        seller2 = people[
            "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Haseltine%2C%20Charles%20Field"
        ]

        buyer1 = people["tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Bergaud%2C%20Georges"]
        buyer2 = people["tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Petit%2C%20Georges"]

        self.assertDictEqual(classification_tree(seller1), {})
        self.assertDictEqual(classified_identifiers(seller1), {"Personal Name": "S. Fabre"})

        self.assertDictEqual(classification_tree(seller2), {})
        self.assertDictEqual(
            classified_identifiers(seller2),
            {"Personal Name": "C. F. Haseltine", "Primary Name": "Haseltine, Charles Field"},
        )

        self.assertEqual(len(seller2["exact_match"]), 1)  # has ulan link
        self.assertEqual(seller2["exact_match"][0]["id"], "http://vocab.getty.edu/ulan/500447562")

        self.assertDictEqual(classification_tree(buyer1), {})
        self.assertDictEqual(
            classified_identifiers(buyer1), {"Personal Name": "G. Bergaud", "Primary Name": "Bergaud, Georges"}
        )

        self.assertEqual(len(buyer1["exact_match"]), 1)  # has ulan link
        self.assertEqual(buyer1["exact_match"][0]["id"], "http://vocab.getty.edu/ulan/500443432")

        self.assertDictEqual(classification_tree(buyer2), {})
        self.assertDictEqual(
            classified_identifiers(buyer2), {"Personal Name": "[for Georges Petit]", "Primary Name": "Petit, Georges"}
        )

        self.assertEqual(len(buyer2["exact_match"]), 1)  # has ulan link
        self.assertEqual(buyer2["exact_match"][0]["id"], "http://vocab.getty.edu/ulan/500447929")


if __name__ == "__main__":
    unittest.main()
