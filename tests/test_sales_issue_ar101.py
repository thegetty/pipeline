#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR101(TestSalesPipelineOutput):
    def test_modeling_ar101(self):
        '''
        AR-101: Replace Repository Number Type with Star Identifier Type
        '''
        output = self.run_pipeline('ar101')
        lo = output['model-lo']
        people = output['model-person']

        catalog = lo['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,SC-A40']
        
        self.assertEqual(classified_identifier_sets(catalog), {
            None: {'Sale Catalog SC-A40'},
            'Owner-Assigned Number': {'SCANDICATS-57', 'SC-A40'},
            'STAR Identifier': {'13209'}
        })
        
        person = people['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,MONOGRAMMIST%20F.Y.']


if __name__ == '__main__':
    unittest.main()
