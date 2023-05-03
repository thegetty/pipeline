#!/usr/bin/env python3 -B
import unittest

from tests import TestGoupilPipelineOutput, classification_tree, classification_sets
from cromulent import vocab

vocab.add_attribute_assignment_check()


class PIRModelingTest_AR235(TestGoupilPipelineOutput):
    def test_modeling_attribution_modifiers(self):
        """
        AR-235 : Physical Object Modifiers
        """
        output = self.run_pipeline("ar235")
        objects = output["model-object"]

        attributed_to = "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Object,g-object-7045"
        formerly_attributed_to = "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Object,g-object-17591"
        copy_by = "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Object,g-object-29955"
        copy_after = "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Object,g-object-9302"
        school_of = "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Object,g-object-21568"
        et = "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Object,g-object-28537"
        and_ = "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:goupil#Object,g-object-27046"

        attribution_modifers = objects[attributed_to]["produced_by"]["attributed_by"]
        self.assertEquals(len(attribution_modifers), 1)
        possibly = [attr for attr in attribution_modifers if "Possibly" in classification_tree(attr)]
        self.assertEqual(len(possibly), 1)

        self.assertEquals(possibly[0]["carried_out_by"][0]["_label"], "Goupil et Cie.")
        self.assertEquals(possibly[0]["referred_to_by"][0]["content"], "attributed to")
        self.assertEquals(possibly[0]["classified_as"][0]["id"], "http://vocab.getty.edu/aat/300435722")

        attribution_modifers = objects[formerly_attributed_to]["produced_by"]["attributed_by"]
        self.assertEquals(len(attribution_modifers), 2)

        possibly = [attr for attr in attribution_modifers if "Possibly" in classification_tree(attr)]
        obsolescence = [attr for attr in attribution_modifers if "Obsolete" in classification_tree(attr)]
        self.assertEqual(len(obsolescence), 1)
        self.assertEqual(len(possibly), 1)

        self.assertEquals(possibly[0]["carried_out_by"][0]["_label"], "Goupil et Cie.")
        self.assertEquals(possibly[0]["referred_to_by"][0]["content"], "attributed to")
        self.assertIn("http://vocab.getty.edu/aat/300435722", classification_sets(possibly[0], key="id"))

        self.assertEquals(obsolescence[0]["carried_out_by"][0]["_label"], "Goupil et Cie.")
        self.assertEquals(obsolescence[0]["referred_to_by"][0]["content"], "formerly attributed to")
        self.assertIn("http://vocab.getty.edu/aat/300404908", classification_sets(obsolescence[0], key="id"))

        self.assertEquals(
            objects[copy_by]["produced_by"]["influenced_by"][0]["_label"],
            "Original of Les souliers de Bal & pendant (deux",
        )
        self.assertEquals(
            objects[copy_by]["produced_by"]["part"][0]["carried_out_by"][0]["_label"], "BROCHART, CONSTANT JOSEPH"
        )

        self.assertEquals(
            objects[copy_after]["produced_by"]["influenced_by"][0]["_label"], "Original of Porte[?] de Marie Antoinette"
        )

        self.assertEquals(
            objects[school_of]["produced_by"]["part"][0]["carried_out_by"][0]["_label"], "School of BOUCHER, CHARLES"
        )

        production_persons = [p["carried_out_by"][0]["_label"] for p in objects[et]["produced_by"]["part"]]

        self.assertIn("VOGEL", production_persons)
        self.assertIn("VERBOECKHOVEN, EUGÈNE", production_persons)

        production_persons = [p["carried_out_by"][0]["_label"] for p in objects[and_]["produced_by"]["part"]]

        self.assertIn("QUINAUX, JOSEPH", production_persons)
        self.assertIn("ROCHUSSEN, CHARLES", production_persons)

        # import pdb; pdb.set_trace()


if __name__ == "__main__":
    unittest.main()
