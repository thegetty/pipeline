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
    classification_sets,
)
from cromulent import vocab

vocab.add_attribute_assignment_check()


class PIRModelingTest_AR192(TestGoupilPipelineOutput):
    def test_modeling_ar192(self):
        """
        AR-192 : Add physical object modelling
        """
        output = self.run_pipeline("ar192")
        objects = output["model-object"]

        objs = [
            {
                "id": "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Object,g-object-23908",
                "title": "Le petit puits[?] [rayé]",
                "from": ["Goupil Stock Book 15, Page 264, Row 2"],
                "dimension_statement": "39 X 54 [rayé]",
                "dimensions": ["39", "54"],
                "location": "",
                "current_owner": "",
            },
            {
                "id": "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Object,g-object-14679",
                "title": "Porte de Bourgogne",
                "from": [
                    "Goupil Stock Book 14, Page 23, Row 6",
                    # "Goupil Stock Book 13" TODO how to handle cases like these
                ],
                "dimension_statement": "",
                "dimensions": [],
                "location": "",
                "current_owner": "",
            },
            {
                "id": "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Object,g-object-23434",
                "title": "Le coup de vent Ancien 18583",
                "from": ["Goupil Stock Book 15, Page 235, Row 13"],
                "dimension_statement": "44 X 53.5",
                "dimensions": ["44", "53.5"],
                "location": "",
                "current_owner": "",
            },
            {
                "id": "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Object,g-object-24935",
                "title": "Un herbage dominant des maisons au bord de la mer (Environs du Havre) Robaut n. 1348 Cl. 883",
                "from": ["Goupil Stock Book 15, Page 332, Row 3"],
                "dimension_statement": "23 X 32",
                "dimensions": ["23", "32"],
                "location": "",
                "current_owner": "",
            },
            {
                "id": "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Object,g-object-7694",
                "title": "Hamlet",
                "from": ["Goupil Stock Book 10, Page 185, Row 15"],
                "dimension_statement": "",
                "dimensions": [],
                "location": "New York, NY, USA",
                "current_owner": "Metropolitan Museum of Art (New York, NY, USA)",
            },
            {
                "id": "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Object,g-object-24373",
                "title": "Vaches au pâturage Ancien 14619",
                "from": ["Goupil Stock Book 15, Page 292, Row 5"],
                "dimension_statement": "56.5 X 83",
                "dimensions": ["56.5", "83"],
                "location": "",
                "current_owner": "",
            },
            {
                "id": "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Object,g-object-12669",
                "title": "L'attelage Nivernais",
                "from": [
                    "Goupil Stock Book 1, Page 93, Row 5",
                    "Goupil Stock Book 2, Page 25, Row 4",
                ],
                "dimension_statement": "",
                "dimensions": [],
                "location": "",
                "current_owner": "",
            },
        ]

        for obj in objs:
            self.assertIn(obj["id"], objects)

            po = objects[obj["id"]]
            print(obj["id"])

            labels = [ref["_label"] for ref in po["referred_to_by"] if "_label" in ref]
            contents = [ref["content"] for ref in po["referred_to_by"] if "content" in ref]

            for l in obj["from"]:
                self.assertIn(l, labels)

            if obj["dimension_statement"]:
                self.assertEqual(len(contents), 1)
                self.assertEqual(contents[0], obj["dimension_statement"])
                dims = [dim["value"] for dim in po["dimension"] if "value" in dim]
                self.assertEqual(len(dims), len(obj["dimensions"]))

            shows = [vi["_label"] for vi in po["shows"]]
            self.assertEqual(len(shows), 1)
            self.assertEqual(shows[0], f"Visual Work of “{obj['title']}”")

            if "current_location" in po:
                loc = po["current_location"]["_label"]
                self.assertEqual(loc, obj["location"])

            if "current_owner" in po:
                own = [own["_label"] for own in po["current_owner"]]
                self.assertEqual(len(own), 1)
                self.assertEqual(own[0], obj["current_owner"])

            # TODO complete tests with production attribution modifiers

        physical_books = [
            y for x, y in objects.items() if "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Book," in x
        ]

        for physical_book in physical_books:
            physical_book_number = physical_book["id"].split(",")[-1]
            self.assertEqual(
                physical_book["carries"][0]["id"],
                "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Text,Book," + physical_book_number,
            )


if __name__ == "__main__":
    unittest.main()
