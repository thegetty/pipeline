#!/usr/bin/env python3 -B
import unittest
import os.path

from tests import TestGoupilPipelineOutput, classified_identifiers, classification_sets
from cromulent import vocab

vocab.add_attribute_assignment_check()


class PIRModelingTest_AR215(TestGoupilPipelineOutput):
    def test_modeling_ar215_no_gri_to_page(self):
        """
        AR-215 : Remove GRI row identifiers from pages
        """
        output = self.run_pipeline("ar215")
        textual_works = output["model-lo"]

        pages = {k: p for k, p in textual_works.items() if "Page" in classification_sets(p)}

        text_work_pages = {
            "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Text,Book,2,Page,68": "G-604",
            "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Text,Book,2,Page,3": "G-23",
            "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Text,Book,1,Page,50": "G-1989",
            "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Text,Book,7,Page,12": "G-10845",
        }
        for k, gri in text_work_pages.items():
            self.assertNotIn(gri, classified_identifiers(pages[k]).values())


if __name__ == "__main__":
    unittest.main()
