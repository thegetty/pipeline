#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR74(TestSalesPipelineOutput):
    def test_modeling_ar74(self):
        '''
        AR-74: Change titling strategy for auction event records for "Auction Event X"
        '''
        output = self.run_pipeline('ar74')
        activities = output['model-sale-activity']
        
        auction = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION-EVENT,B-A136']
        self.assertEqual(auction['_label'], 'Auction Event B-A136 (1773-07-20 onwards)')
        self.assertEqual(classified_identifier_sets(auction), {
        	None: {'Auction Event B-A136 (1773-07-20 onwards)'}
        })


if __name__ == '__main__':
    unittest.main()