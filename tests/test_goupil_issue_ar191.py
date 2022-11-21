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


class PIRModelingTest_AR191(TestGoupilPipelineOutput):
    def test_modeling_ar191(self):
        """
        AR-192 : Model Goupil Visual Work
        """
        output = self.run_pipeline("ar191")
        visual_item = output["model-visual-item"][
            "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Object,Internal,G-23884-VisItem"
        ]

        # Title of the visual work
        self.assertEqual(
            visual_item["_label"],
            "Visual work of “Hamlet”",
        )    

        # Classification of the visual work with ulan id
        self.assertEqual(
            visual_item["classified_as"][0]["id"],
            "http://vocab.getty.edu/aat/300033898",
        )

        # Label of the classification
        self.assertEqual(
            visual_item["classified_as"][0]["_label"],
            "Historie",
        )

        # Reference to visual work by the textual work
        self.assertEqual(
            visual_item["referred_to_by"][0]["id"],
            "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Text,Book,10,Page,185,Row,15",
        )

        # Type of the identifier
        self.assertEqual(
            visual_item["identified_by"][0]["classified_as"][0]["id"],
            "http://vocab.getty.edu/aat/300417193",
        )

        # Reference to visual work by the textual work
        self.assertEqual(
            visual_item["identified_by"][0]["referred_to_by"][0]["id"],
            "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Text,Book,10,Page,185,Row,15",
        )

        # Title of the visual work
        self.assertEqual(
            visual_item["identified_by"][0]["content"],
            "Hamlet",
        )


if __name__ == "__main__":
    unittest.main()
