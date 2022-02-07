#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR81(TestSalesPipelineOutput):
    def test_modeling_ar81(self):
        '''
        AR-81: Fix event timespan modeling
        '''
        output = self.run_pipeline('ar81')
        activities = output['model-sale-activity']
        
        # This event has an end-date modifier 'and following days'.
        # Its timespan label should indicate an open range, but the
        # timespan should also include an end-date of 15 days after the start.
        # Per Eric Hormell:
        # 
        # """
        # PSCP thinks we should set an arbitrary end date for the searchable date
        # range for sales with "and following days." We decided that the best
        # option would be to make it 15 days. That would be long enough to safely
        # cover most sales, but not be too long.
        # """
        event = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PRIVATE_CONTRACT_SALE-EVENT,Br-279']
        ts = event['timespan']
        self.assertEqual(ts['_label'], f'1804-06-11 onwards')
        self.assertIn('end_of_the_end', ts)
        self.assertEqual(ts['begin_of_the_begin'], '1804-06-11T00:00:00Z')
        self.assertEqual(ts['end_of_the_end'], '1804-06-26T00:00:00Z')


if __name__ == '__main__':
    unittest.main()
