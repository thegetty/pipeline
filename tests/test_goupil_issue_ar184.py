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
        
    def test_modeling_ar199(self):
        """
        AR-199 : Using the pscp_benchmark file, test the group entities
        """
        output = self.run_pipeline("pscp_benchmark")
        groups = output["model-groups"]

        # If the group has a primary name, is should not be referred by a textual work
        def primary_name_reference(data: dict):
            for identifier in data["identified_by"]:
                try:
                    if identifier["classified_as"][0]["id"] == "http://vocab.getty.edu/aat/300404670":
                        self.assertNotIn("referred_to_by", identifier)
                except:
                    pass
        # If the group has a corporate name, is referred by a textual work
        def corporate_name_reference(data: dict):
            identifierTextualWorkReferencesCounter = 0
            for identifier in data["identified_by"]:
                try:
                    if identifier["classified_as"][0]["id"] == "http://vocab.getty.edu/aat/300445020":
                        self.assertIn("referred_to_by", identifier)
                        identifierTextualWorkReferencesCounter += len(identifier["referred_to_by"])
                except:
                    pass
            # The amount of references of textual works to identifiers must be equal to the amount of references of textual works to the group entity
            self.assertEqual(len(data.get("referred_to_by", [])), identifierTextualWorkReferencesCounter)

        self.assertEqual(len(groups), 16)

        for group in [y for x, y in groups.items() if "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH" in x]:
           primary_name_reference(group)
           corporate_name_reference(group)



if __name__ == "__main__":
    unittest.main()
