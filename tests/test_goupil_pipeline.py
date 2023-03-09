#!/usr/bin/env python3 -B
import unittest
import os
import os.path
import hashlib
import json
import uuid
import pprint

from tests import TestWriter, GoupilTestPipeline, MODELS, classification_tree
from cromulent import vocab

vocab.add_attribute_assignment_check()


class TestGoupilPipelineOutput(unittest.TestCase):
    """
    Parse test CSV data and run the Goupil pipeline with the in-memory TestWriter.
    Then verify that the serializations in the TestWriter object are what was expected.
    """

    def setUp(self):
        self.data = {
            "header_file": "tests/data/goupil/goupil_0.csv",
            "files_pattern": "tests/data/goupil/goupil.csv",
        }
        os.environ["QUIET"] = "1"

    def tearDown(self):
        pass

    def run_pipeline(self, models, input_path):
        writer = TestWriter()
        pipeline = GoupilTestPipeline(writer, input_path, data=self.data, models=models, limit=10, debug=True)
        pipeline.run()
        return writer.processed_output()

    def verify_auction(self, a, event, idents):
        got_events = {c["_label"] for c in a.get("part_of", [])}
        self.assertEqual(got_events, {f"Auction Event {event}"})
        got_idents = {c["content"] for c in a.get("identified_by", [])}
        self.assertEqual(got_idents, idents)

    def test_pipeline_goupil(self):
        input_path = os.getcwd()
        models = MODELS
        output = self.run_pipeline(models, input_path)

        # TODO : As more modelling is done the following should be updated as well, so that eventually all serialization for all models will be checked
        los = output["model-lo"]
        groups = output["model-groups"]

        # import pdb;
        # pdb.set_trace()

        # more data produce more objects los, right? ;p
        self.assertEqual(len(los), 19)
        self.assertEqual(len(groups), 7)
        goupil = groups["tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Goupil%20et%20Cie."]

        self.assertEqual(goupil["_label"], "Goupil et Cie.")

        lo1 = los["tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Text,Book,15"]
        lo2 = los["tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Text,Book,15,Page,63"]
        lo3 = los["tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Text,Book,15,Page,63,Row,3"]
        lo4 = los["tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Text,Book,14"]

        # import pdb

        # pdb.set_trace()

        self.assertDictEqual(
            classification_tree(lo1),
            {
                "Account Book": {"Type of Work": {}},
                "Book": {"Form": {}},
            },
        )
        self.assertDictEqual(
            classification_tree(lo1, key="id"),
            {
                "http://vocab.getty.edu/aat/300027483": {"http://vocab.getty.edu/aat/300435443": {}},
                "http://vocab.getty.edu/aat/300028051": {"http://vocab.getty.edu/aat/300444970": {}},
            },
        )

        self.assertDictEqual(
            classification_tree(lo4),
            {
                "Account Book": {"Type of Work": {}},
                "Book": {"Form": {}},
            },
        )
        self.assertDictEqual(
            classification_tree(lo4, key="id"),
            {
                "http://vocab.getty.edu/aat/300027483": {"http://vocab.getty.edu/aat/300435443": {}},
                "http://vocab.getty.edu/aat/300028051": {"http://vocab.getty.edu/aat/300444970": {}},
            },
        )

        self.assertDictEqual(
            classification_tree(lo2),
            {
                "Account Book": {"Type of Work": {}},
                "Page": {"Form": {}},
            },
        )
        self.assertDictEqual(
            classification_tree(lo2, key="id"),
            {
                "http://vocab.getty.edu/aat/300027483": {"http://vocab.getty.edu/aat/300435443": {}},
                "http://vocab.getty.edu/aat/300194222": {"http://vocab.getty.edu/aat/300444970": {}},
            },
        )

        self.assertDictEqual(
            classification_tree(lo3),
            {
                "Account Book": {"Type of Work": {}},
                "Entry": {"Form": {}},
            },
        )
        self.assertDictEqual(
            classification_tree(lo3, key="id"),
            {
                "http://vocab.getty.edu/aat/300027483": {"http://vocab.getty.edu/aat/300435443": {}},
                "http://vocab.getty.edu/aat/300438434": {"http://vocab.getty.edu/aat/300444970": {}},
            },
        )

        self.assertEqual(len(lo3["referred_to_by"]), 1)
        # page is part of book
        self.assertEqual(lo1["id"], lo2.get("part_of")[0]["id"])
        # row is part of page
        self.assertEqual(lo2["id"], lo3.get("part_of")[0]["id"])

        self.assertEqual(lo1.get("created_by").get("carried_out_by")[0]["id"], goupil["id"])
        self.assertEqual(lo2.get("created_by").get("carried_out_by")[0]["id"], goupil["id"])
        self.assertEqual(lo3.get("created_by").get("carried_out_by")[0]["id"], goupil["id"])
        self.assertEqual(lo4.get("created_by").get("carried_out_by")[0]["id"], goupil["id"])

        physical_book_15_id = "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Book,15"
        physical_book_15 = output["model-object"][physical_book_15_id]
        physical_book_14_id = "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Book,14"
        physical_book_14 = output["model-object"][physical_book_14_id]
        # Connection of lo book to physical book
        self.assertEqual(physical_book_15.get("carries")[0]["id"], lo1.get("id"))
        self.assertEqual(physical_book_14.get("carries")[0]["id"], lo4.get("id"))


if __name__ == "__main__":
    unittest.main()
