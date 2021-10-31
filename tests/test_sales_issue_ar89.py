#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifiers

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR89(TestSalesPipelineOutput):
    def test_modeling_ar89(self):
        '''
        AR-89: Naming of groups representing an uncertain member
        '''
        output = self.run_pipeline('ar89')
        groups = output['model-groups']
        group = groups['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PROV,B-183,1810-10-26,0050-SellerGroup']
        self.assertIn('identified_by', group)
        group_names = group['identified_by']
        self.assertEqual(len(group_names), 1)
        group_name = group_names[0]
        self.assertEqual(group_name['content'], 'Molo, Pierre-Louis de OR Blaere, Cathérine, Dame')


if __name__ == '__main__':
    unittest.main()
