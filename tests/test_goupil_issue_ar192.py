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


class PIRModelingTest_AR192(TestGoupilPipelineOutput):
    def test_modeling_ar192(self):
        """
        AR-192 : Add physical object modelling
        """
        output = self.run_pipeline("ar192")
        objects = output["model-object"]

        object = objects[
            "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Object,Internal,G-42810"
        ]

        self.assertEqual(len(object["identified_by"]), 3)

        self.assertEqual(
            object["referred_to_by"][0]["id"],
            "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Text,Book,15,Page,264,Row,2",
        )

        self.assertEqual(object["dimension"][0]["type"], "Dimension")
        self.assertEqual(object["dimension"][0]["value"], 39)
        self.assertEqual(
            object["shows"][0]["_label"], "Visual work of “Le petit puits[?] [rayé]”"
        )

        object = objects[
            "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Object,Internal,G-23884"
        ]

        self.assertEqual(
            object["current_location"]["_label"],
            "New York, NY, USA",
        )

        self.assertEqual(
            object["current_owner"][0]["_label"],
            "Metropolitan Museum of Art (New York, NY, USA)",
        )

        production = object["produced_by"]
        self.assertEqual(
            production["_label"],
            "Production event for Hamlet",
        )

        production_sub_event = production["part"][0]
        self.assertEqual(
            production_sub_event["_label"],
            "Production sub-event for DELACROIX, EUGÈNE",
        )
        self.assertEqual(
            production_sub_event["carried_out_by"][0]["_label"],
            "DELACROIX, EUGÈNE",
        )

    def test_modeling_ar192_2(self):
        """
        AR-192 : Add physical object modelling, test for relation linguistic object about physical object
        """
        output = self.run_pipeline("ar192")
        linguisticObject = output["model-lo"][
            "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Text,Book,15,Page,264,Row,2"
        ]

        self.assertEqual(
            linguisticObject["about"][0]["_label"], "Le petit puits[?] [rayé]"
        )

    def test_modeling_ar192_3(self):
        """
        AR-192 : Add physical object modelling, test for relation linguistic object about physical object
        """
        output = self.run_pipeline("ar192")
        object = output["model-object"][
            "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Object,Internal,G-42810"
        ]

        self.assertEqual(
            object["produced_by"]["attributed_by"][0]["_label"], "Formerly attributed to BONHEUR, ROSA"
        )

        self.assertEqual(
            object["produced_by"]["attributed_by"][0]["classified_as"][0]["_label"], "Obsolete"
        )


if __name__ == "__main__":
    unittest.main()
