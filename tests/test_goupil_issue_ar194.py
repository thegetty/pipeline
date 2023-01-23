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

    def test_modeling_ar194_2(self):
        """
        AR-194 : Model Provenance Activity buy_sell_attribution_modifiers
        """
        output = self.run_pipeline("ar194")
        activities = output["model-activity"]

        out_with_agents = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#TX,Out,14,23,6']

        payment = out_with_agents["part"][0]
        self.assertTrue({'paid_amount', 'paid_from', 'paid_to', 'part'}.issubset(set(payment.keys())))
        agents_activity = payment["part"][0]
        self.assertEqual(agents_activity["type"], "Activity")
        self.assertEqual(agents_activity["_label"], "Buyer's agent's role in payment")
        self.assertDictEqual(classification_tree(agents_activity), {"Buyer's Agent": {}})
        self.assertEqual(agents_activity["carried_out_by"][0]["id"], 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Petit%2C%20Georges')
        acquisition = out_with_agents["part"][1]
        self.assertTrue({'transferred_title_from', 'transferred_title_of','transferred_title_to', 'part'}.issubset(set(acquisition.keys())))
        agents_activity = payment["part"][0]
        self.assertEqual(agents_activity["type"], "Activity")
        self.assertEqual(agents_activity["_label"], "Buyer's agent's role in payment")
        self.assertDictEqual(classification_tree(agents_activity), {"Buyer's Agent": {}})
        self.assertEqual(agents_activity["carried_out_by"][0]["id"], 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Petit%2C%20Georges')

    def test_modeling_ar194_3(self):
        """
        AR-194 : Model Provenance Activity Places coming from sellers and buyers locations
        """
        output = self.run_pipeline("ar194")
        place = output["model-place"]

        self.assertEqual(len(place), 5)

        paris = place['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PLACE,France,Paris']
        self.assertTrue({'classified_as', 'identified_by', 'referred_to_by'}.issubset(set(paris.keys())))

        self.assertTrue(len(paris["referred_to_by"]), 2)


    def test_modeling_ar194_4(self):
        """
        AR-194 : Model Provenance Activity #TODO Add modeling for the different transactions
        """


if __name__ == "__main__":
    unittest.main()
