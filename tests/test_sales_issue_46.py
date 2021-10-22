#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR46(TestSalesPipelineOutput):
    '''
    AR-46: Sales Event Dates
    '''
    def test_modeling_ar46(self):
        output = self.run_pipeline('ar46')

        objects = output['model-object']
        groups = output['model-groups']
        activities = output['model-activity']
        sale_act = output['model-sale-activity']
        
        # lot sale date ('+' modified), event begin date, NO end date
        event1 = sale_act['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PRIVATE_CONTRACT_SALE-EVENT,Br-279']
        sale1 = sale_act['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION,Br-279,0103,1804-06-11']
        self.assertIn('1804-06-11 onwards', classified_identifier_sets(sale1['timespan'])[None])
        self.assertEqual(
        	classified_identifier_sets(sale1['timespan']),
        	classified_identifier_sets(event1['timespan']),
        )

        # lot sale date ('+' modified), event begin date, event end date
        event2 = sale_act['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PRIVATE_CONTRACT_SALE-EVENT,Br-64']
        sale2 = sale_act['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION,Br-64,0001,1801-11-16']
        self.assertIn('1801-11-16 to 1803-01-01', classified_identifier_sets(sale2['timespan'])[None])
        self.assertEqual(
        	classified_identifier_sets(sale2['timespan']),
        	classified_identifier_sets(event2['timespan']),
        )

        # lot sale date ('+' modified, zero-day), event begin date (zero-day), NO end date
        # in this case, the lot sale date and the event begin date are the same (both with a zero-valued day field).
        # as a result, the event keeps the verbatim date string as the Name (with the trailing "-00"), but
        # that wasn't asserted on the lot sale, so we use the shortened form to indicate the full month ("1808-07").
        event3 = sale_act['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PRIVATE_CONTRACT_SALE-EVENT,Br-608-A']
        sale3 = sale_act['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION,Br-608-A,0449,1808-07']
        self.assertIn('1808-07-00', classified_identifier_sets(event3['timespan'])[None])
        self.assertIn('1808-07', classified_identifier_sets(sale3['timespan'])[None])


if __name__ == '__main__':
    unittest.main()
