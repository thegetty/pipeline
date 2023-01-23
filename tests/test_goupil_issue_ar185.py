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


class PIRModelingTest_AR185(TestGoupilPipelineOutput):
    def test_modeling_ar185(self):
        """
        AR-185 : Add person modelling for Goupil Artists
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
        self.assertIn(
            "Goupil Stock Book 15, Page 264, Row 2",
            classification_sets(art1, key="_label", classification_key="referred_to_by"),
        )

        self.assertDictEqual(classification_tree(art2), {"French": {"Nationality": {}}})
        self.assertDictEqual(
            classified_identifiers(art2), {"Personal Name": "Lemmens", "Primary Name": "LEMMENS, EMILE"}
        )

        self.assertEqual(len(art2["exact_match"]), 1)  # has ulan link
        self.assertEqual(art2["exact_match"][0]["id"], "http://vocab.getty.edu/ulan/500099755")
        self.assertIn(
            "Goupil Stock Book 15, Page 264, Row 2",
            classification_sets(art2, key="_label", classification_key="referred_to_by"),
        )

    def test_modeling_ar185_2(self):
        """
        AR-185 : Add person modelling for Sellers and Buyers
        """
        output = self.run_pipeline("ar185")
        people = output["model-person"]
        # When the transaction is: "Returned" both Name and Personal Name are created
        # seller1 = people["tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#PERSON,PI,G-42810,seller_1"]
        seller1 = people["tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#PERSON,PI,G-43741,seller_1"]
        seller2 = people[
            "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Haseltine%2C%20Charles%20Field"
        ]

        buyer1 = people["tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Bergaud%2C%20Georges"]
        buyer2 = people["tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Petit%2C%20Georges"]

        # When the transaction is: "Returned" both Name and Personal Name are created
        # self.assertDictEqual(classification_tree(seller1), {})

        # self.assertDictEqual(classified_identifiers(seller1), {"Personal Name": "S. Fabre"})
        # self.assertDictEqual(classified_identifiers(seller1), {"Personal Name": "S. Fabre", None: "S. Fabre"})

        # self.assertIn(
        #     "Goupil Stock Book 15, Page 264, Row 2",
        #     classification_sets(seller1, key="_label", classification_key="referred_to_by"),
        # )
        self.assertDictEqual(classification_tree(seller1), {})

        self.assertDictEqual(classified_identifiers(seller1), {"Personal Name": "C. Honet"})

        self.assertIn(
            "Goupil Stock Book 15, Page 332, Row 3",
            classification_sets(seller1, key="_label", classification_key="referred_to_by"),
        )

        self.assertDictEqual(classification_tree(seller2), {})
        self.assertDictEqual(
            classified_identifiers(seller2),
            {"Personal Name": "C. F. Haseltine", "Primary Name": "Haseltine, Charles Field"},
        )
        self.assertIn(
            "Goupil Stock Book 14, Page 23, Row 6",
            classification_sets(seller2, key="_label", classification_key="referred_to_by"),
        )
        self.assertEqual(len(seller2["exact_match"]), 1)  # has ulan link
        self.assertEqual(seller2["exact_match"][0]["id"], "http://vocab.getty.edu/ulan/500447562")

        self.assertDictEqual(classification_tree(buyer1), {})
        self.assertDictEqual(
            classified_identifiers(buyer1), {"Personal Name": "G. Bergaud", "Primary Name": "Bergaud, Georges"}
        )
        self.assertIn(
            "Goupil Stock Book 14, Page 23, Row 6",
            classification_sets(buyer1, key="_label", classification_key="referred_to_by"),
        )
        self.assertEqual(len(buyer1["exact_match"]), 1)  # has ulan link
        self.assertEqual(buyer1["exact_match"][0]["id"], "http://vocab.getty.edu/ulan/500443432")

        self.assertDictEqual(classification_tree(buyer2), {})
        self.assertDictEqual(
            classified_identifiers(buyer2), {"Personal Name": "[for Georges Petit]", "Primary Name": "Petit, Georges"}
        )
        self.assertIn(
            "Goupil Stock Book 14, Page 23, Row 6",
            classification_sets(buyer2, key="_label", classification_key="referred_to_by"),
        )
        self.assertEqual(len(buyer2["exact_match"]), 1)  # has ulan link
        self.assertEqual(buyer2["exact_match"][0]["id"], "http://vocab.getty.edu/ulan/500447929")

    def test_modeling_ar185_3(self):
        """
        AR-185 : Add person modelling for prev and post buyers
        """
        output = self.run_pipeline("ar185")
        people = output["model-person"]

        previous_owners_of_23884 = [y for x, y in people.items() if "PERSON,PI,G-23884,prev_own_" in x]
        post_owners_of_23884 = [y for x, y in people.items() if "PERSON,PI,G-23884,post_own_" in x]

        self.assertEqual(len(previous_owners_of_23884), 2)
        self.assertEqual(len(post_owners_of_23884), 4)

        person1 = people["tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#PERSON,PI,G-23884,post_own_1"]
        person2 = people["tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#PERSON,PI,G-23884,prev_own_1"]

        self.assertDictEqual(classification_tree(person1), {})

        self.assertDictEqual(
            classified_identifiers(person1), {"Personal Name": "Samuel P. Avery, New York (from 1881, sold to Smith)"}
        )

        self.assertIn(
            "Goupil Stock Book 10, Page 185, Row 15",
            classification_sets(person1, key="_label", classification_key="referred_to_by"),
        )

        self.assertDictEqual(classification_tree(person2), {})

        self.assertDictEqual(classified_identifiers(person2), {"Personal Name": "Narcisse-Virgile Diaz de la Peña"})

        self.assertIn(
            "Goupil Stock Book 10, Page 185, Row 15",
            classification_sets(person2, key="_label", classification_key="referred_to_by"),
        )

    def test_modeling_ar185_4(self):
        """
        AR-185 : Add person modelling for joint ownership
        """
        output = self.run_pipeline("ar185")
        people = output["model-person"]

        person1 = people[
            "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#PERSON,PI,G-43741,shared-buyer_1"
        ]

        # self.assertEqual(person1["_label"], "G. Petit")
        # self.assertEqual(person1["referred_to_by"][0]["_label"], "Goupil Stock Book 15, Page 332, Row 3")

    def test_modeling_ar185_5(self):
        """
        AR-185 : Authority names should not have name source reference
        """
        output = self.run_pipeline("ar185")
        people = output["model-person"]

        authorityPeople = [y for x, y in people.items() if ",AUTH," in x]

        def preferred_name(data: dict):
            for identifier in data["identified_by"]:
                if identifier["classified_as"][0]["id"] == "http://vocab.getty.edu/aat/300404670":
                    return identifier
            return []

        for authorityPerson in authorityPeople:
            self.assertNotIn("referred_to_by", preferred_name(authorityPerson))

    def test_modeling_ar185_5(self):
        """
        AR-185 : Buyers and Sellers sojourn activity
        """
        output = self.run_pipeline("ar185")
        people = output["model-person"]

        knoedler = people["tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Knoedler%27s"]
        for activity in knoedler["carried_out"]:
            self.assertDictEqual(classification_tree(activity), {"Preferred Terms": {}})
            self.assertTrue({"took_place_at", "classified_as", "referred_to_by"}.issubset(set(activity.keys())))


if __name__ == "__main__":
    unittest.main()
