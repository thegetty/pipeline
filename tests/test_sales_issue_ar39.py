#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifiers

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR39(TestSalesPipelineOutput):
    def test_modeling_ar39(self):
        '''
        AR-39: Ensure names of stock books and auction catalogs match "Sale Catalog" style
        '''
        output = self.run_pipeline('ar39')
        lo = output['model-lo']
        objects = output['model-object']

        # Catalog Textual Works
        expected_br = 'Sale Catalog Br-1544'
        cat_br = lo['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,Br-1544']
        self.assertEqual(cat_br['_label'], expected_br)
        self.assertEqual(classified_identifiers(cat_br)[None], expected_br)

        expected_sc = 'Sale Catalog SC-A40'
        cat_sc = lo['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,SC-A40']
        self.assertEqual(cat_sc['_label'], expected_sc)
        self.assertEqual(classified_identifiers(cat_sc)[None], expected_sc)

        expected_d = 'Sale Catalog D-A50'
        cat_d = lo['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,D-A50']
        self.assertEqual(cat_d['_label'], expected_d)
        self.assertEqual(classified_identifiers(cat_d)[None], expected_d)

        # Physical Catalogs
        expected_br_bib = 'Sale Catalog Br-1544, owned by “BIB”'
        phy_bib = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PHYS-CAT,Br-1544,BIB']
        self.assertEqual(phy_bib['_label'], expected_br_bib)
        self.assertEqual(classified_identifiers(phy_bib)[None], expected_br_bib)

        expected_br_val = 'Sale Catalog Br-1544, owned by “VAL”'
        phy_val = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PHYS-CAT,Br-1544,VAL']
        self.assertEqual(phy_val['_label'], expected_br_val)
        self.assertEqual(classified_identifiers(phy_val)[None], expected_br_val)


if __name__ == '__main__':
    unittest.main()
