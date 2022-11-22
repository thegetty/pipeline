#!/usr/bin/env python3 -B
import unittest
import os
import os.path
import hashlib
import json
import uuid
import pprint

from tests import TestWriter, TestGoupilPipelineOutput, MODELS, classified_identifiers, classification_tree, classification_sets
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

        self.assertDictEqual(classification_tree(visual_item), {'Historie': {}})
        self.assertDictEqual(classified_identifiers(visual_item), {'Title': 'Hamlet'})
        self.assertEqual(classification_sets(visual_item, key='id'), {'http://vocab.getty.edu/aat/300033898'})


        # Classification of Identifiers
        self.assertDictEqual(classification_tree(visual_item["identified_by"][0]), {'Title': {}})

        # Reference to visual work by the textual work
        textualWorkReferences = []
        for references in visual_item.get("referred_to_by"):
            textualWorkReferences.append(references.get("id"))

        # Reference to the title of the visual work by the textual work
        for identifiers in visual_item.get("identified_by"):
            for references in identifiers.get("referred_to_by"):
                textualWorkReferences.append(references.get("id"))


        self.assertTrue(len(textualWorkReferences)==2)
        self.assertTrue(textualWorkReferences[0] == textualWorkReferences[1] == "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Text,Book,10,Page,185,Row,15")


if __name__ == "__main__":
    unittest.main()
