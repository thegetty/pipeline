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
    classified_identifier_sets,
    classification_tree,
    classification_sets,
)
from cromulent import vocab

vocab.add_attribute_assignment_check()


class PIRModelingTest_AR191(TestGoupilPipelineOutput):
    def test_modeling_ar191(self):
        """
        AR-191 : Model Goupil Visual Work
        """
        output = self.run_pipeline("ar191")
        visual_items = output["model-visual-item"]

        vis = [
            {
                "id": "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Object,g-object-23908-VisItem",
                "title": "Le petit puits[?] [rayé]",
                "from": ["Goupil Stock Book 15, Page 264, Row 2"],
                "classification": {"http://vocab.getty.edu/aat/300015636"},
                "depicts": {},
            },
            {
                "id": "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Object,g-object-14679-VisItem",
                "title": "Porte de Bourgogne",
                "from": [
                    "Goupil Stock Book 14, Page 23, Row 6",
                    # "Goupil Stock Book 13" TODO how to handle cases like these
                ],
                "classification": {"http://vocab.getty.edu/aat/300015636"},
                "depicts": {},
            },
            {
                "id": "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Object,g-object-23434-VisItem",
                "title": "Le coup de vent Ancien 18583",
                "from": ["Goupil Stock Book 15, Page 235, Row 13"],
                "classification": {"http://vocab.getty.edu/aat/300139140"},
                "depicts": {},
            },
            {
                "id": "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Object,g-object-24935-VisItem",
                "title": "Un herbage dominant des maisons au bord de la mer (Environs du Havre) Robaut n. 1348 Cl. 883",
                "from": ["Goupil Stock Book 15, Page 332, Row 3"],
                "classification": {"http://vocab.getty.edu/aat/300015636"},
                "depicts": {},
            },
            {
                "id": "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Object,g-object-7694-VisItem",
                "title": "Hamlet",
                "from": ["Goupil Stock Book 10, Page 185, Row 15"],
                "classification": {"http://vocab.getty.edu/aat/300386045"},
                "depicts": {},
            },
            {
                "id": "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Object,g-object-24373-VisItem",
                "title": "Vaches au pâturage Ancien 14619",
                "from": ["Goupil Stock Book 15, Page 292, Row 5"],
                "classification": {"http://vocab.getty.edu/aat/300015636"},
                "depicts": {},
            },
            {
                "id": "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Object,g-object-12669-VisItem",
                "title": "L'attelage Nivernais",
                "from": ["Goupil Stock Book 1, Page 93, Row 5", "Goupil Stock Book 2, Page 25, Row 4"],
                "classification": {"http://vocab.getty.edu/aat/300139140"},
                "depicts": {},
            },
            {
                "id": "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Object,g-object-13776-VisItem",
                "title": "Les fortifications Effet de neige",
                "from": ["Goupil Stock Book 12, Page 192, Row 14"],
                "classification": {"http://vocab.getty.edu/aat/300015636"},
                "depicts": {},
            },
            {
                "id": "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Object,g-object-22904-VisItem",
                "title": "Scène militaire",
                "from": ["Goupil Stock Book 15, Page 203, Row 14"],
                "classification": {"http://vocab.getty.edu/aat/300139140"},
                "depicts": {"http://vocab.getty.edu/aat/300025450"},
            },
        ]

        for v in vis:
            vi = visual_items[v["id"]]

            self.assertEqual(f"Visual Work of “{v['title']}”", vi["_label"])
            self.assertEqual(
                v["title"],
                classified_identifiers(vi)["Title"],
            )

            self.assertEqual(
                ["http://vocab.getty.edu/aat/300417193"], list(classified_identifiers(vi, member="id").keys())
            )

            referred_to_by = []
            for identifier in vi["identified_by"]:
                for ref in identifier["referred_to_by"]:
                    referred_to_by.append(ref)

            self.assertEqual(len(v["from"]), len(referred_to_by))
            for v["from"] in v["from"]:
                self.assertIn(v["from"], [l["_label"] for l in referred_to_by])

            self.assertEqual(v["classification"], classification_sets(vi, key="id"))
            if v["depicts"]:
                self.assertEqual(
                    v["depicts"], classification_sets(vi, key="id", classification_key="represents_instance_of_type")
                )


if __name__ == "__main__":
    unittest.main()
