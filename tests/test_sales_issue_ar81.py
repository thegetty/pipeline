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
        sets = output['model-set']
        
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
        
        # This sale is part of an event with an end-date modifier 'and following days'.
        # Its timespan should include an end-date matching the event.
        sale = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION,Br-A2042,0047,1794-05-12']
        ts = sale['timespan']
        self.assertEqual(ts['_label'], f'1794-05-12 to 1794-05-26')
        self.assertIn('end_of_the_end', ts)
        self.assertEqual(ts['begin_of_the_begin'], '1794-05-12T00:00:00Z')
        self.assertEqual(ts['end_of_the_end'], '1794-05-27T00:00:00Z')
        
        # Ensure that the object set corresponding to the sale also has the same timespan identifier
        objset = sets['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION,Br-A2042,0047,1794-05-12-Set']
        creation = objset['created_by']
        ts = creation['timespan']
        self.assertIn('1794-05-12 onwards', classified_identifier_sets(ts)[None])


if __name__ == '__main__':
    unittest.main()
