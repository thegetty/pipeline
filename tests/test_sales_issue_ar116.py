#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR116(TestSalesPipelineOutput):
    def test_modeling_ar116(self):
        '''
        AR-116: Add type to linguistic statement to disambiguate if the statement is about buyer or seller
        '''
        output = self.run_pipeline('ar116')
        activities = output['model-activity']
        
        sale = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PROV,B-183,1810-10-26,0050']
        acqs = [p for p in sale.get('part', []) if p['type'] == 'Acquisition']
        self.assertEqual(len(acqs), 1)
        acq = acqs[0]

        self.assertEqual(classified_identifier_sets(acq, key='referred_to_by'), {
            'Note': {'and', 'or'},
            'Qualifier': {'and', 'or'},
            'Buyer description': {'and'},
            'Seller description': {'or'}})


if __name__ == '__main__':
    unittest.main()
