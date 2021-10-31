#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR75(TestSalesPipelineOutput):
    def test_modeling_ar75(self):
        '''
        AR-75: Change text created in 'sale leading to the previous ownership' style Provenance Activity records
        '''
        output = self.run_pipeline('ar75')
        activities = output['model-activity']
        
        event = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PROV,Seller-0,OBJ,B-447,0013,1827-05-22']
        self.assertEqual(classified_identifier_sets(event, key='referred_to_by'), {
        	'Source Statement': {'Listed as the seller of object in B-447 0013 (1827-05-22) that was withdrawn'}
        })

if __name__ == '__main__':
    unittest.main()
