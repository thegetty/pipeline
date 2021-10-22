#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR44(TestSalesPipelineOutput):
    def test_modeling_ar44(self):
        '''
        AR-44: Carry over Title Modifier in Sales records
        '''
        output = self.run_pipeline('ar44')
        objects = output['model-object']

        obj1 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,B-A138,0023,1774-05-30']
        obj2 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,B-A139,0119%5Ba%5D,1774-05-31']
        obj3 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,B-A139,0194%5Ba%5D,1774-05-31']
        obj4 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B1153,0320%5Ba%5D,1908-04-08']

        # obj1 has a title modifier that DOES NOT match the set of prefixes used for asserting an extra title
        # instead, it is asserted as a 'Title Statement' referring to the object
        obj1_ids = classified_identifier_sets(obj1)
        obj1_refs = classified_identifier_sets(obj1, key='referred_to_by')
        obj1_titles = obj1_ids['Title']
        self.assertEqual(obj1_titles, {
            "Un très-beau Tableau, peint par David Teniers, d'après un Maître Italien, représentant Jesus-Christ…"
        })
        self.assertEqual(obj1_refs['Note'], {'copy by Teniers, David after Italian'})

        # obj2 has a title modifier with the prefix 'THIS LOT:'
        obj2_ids = classified_identifier_sets(obj2)
        obj2_titles = obj2_ids['Title']
        self.assertDictContainsSubset({
            'Primary Name': {"Un Tableau, à Fruits &c.; lot 119[b] in the style of Vinckebooms"}
        }, obj2_ids)
        self.assertEqual(obj2_titles, {
            "Un Tableau, à Fruits &c.; lot 119[b] in the style of Vinckebooms",
            "Un Tableau, à Fruits &c., par Gillemans.  Un Païsage dans le goût de Vinckebooms"
        })

        # obj3 has a title modifier with the prefix 'Sous ce no.:'
        obj3_ids = classified_identifier_sets(obj3)
        obj3_titles = obj3_ids['Title']
        self.assertDictContainsSubset({
            'Primary Name': {"Un Paisage ; lot no.194[b] style of Boudewyns"}
        }, obj3_ids)
        self.assertEqual(obj3_titles, {
            "Un Paisage ; lot no.194[b] style of Boudewyns",
            "Deux Païsages, l'un par Artois, & l'autre maniere de Baudewyns"
        })

        # obj4 has a title modifier with the prefix 'DIESES LOS:'
        obj4_ids = classified_identifier_sets(obj4)
        obj4_titles = obj4_ids['Title']
        self.assertDictContainsSubset({
            'Primary Name': {"Frauenbildnis; H. 17, B.12BD cm"}
        }, obj4_ids)
        self.assertEqual(obj4_titles, {
            "Frauenbildnis; H. 17, B.12BD cm",
            "2 Bl. Frauenbildnisse. H. 17 und 18, B. je 12BD cm. Aquarelle."
        })

if __name__ == '__main__':
    unittest.main()
