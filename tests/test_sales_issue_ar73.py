#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifiers

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR73(TestSalesPipelineOutput):
    def test_modeling_ar73(self):
        '''
        AR-73: Change Phrasing of Title for Provenance Events
        '''
        output = self.run_pipeline('ar73')
        sales = output['model-sale-activity']
        activities = output['model-activity']
        
        prev1 = 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PROV-post-owner-1,OBJ,B-A13,0339,1738-07-21'
        self.assertIn(prev1, activities)
        e1 = activities[prev1]
        self.assertEqual(e1['_label'], 'Event leading to Ownership of B-A13 0339 (1738-07-21)')

        prev2 = 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PROV,Seller-0,OBJ,B-A92,0028,1767-11-16'
        self.assertIn(prev2, activities)
        e2 = activities[prev2]
        self.assertEqual(e2['_label'], 'Event leading to Ownership of B-A92 0028 (1767-11-16)')
        
        # This record wasn't sold (transaction type is "Unknown"), so there's no primary transaction
        # or bidding that relates to the previous provenance event. So we just reference it directly
        # and make sure that it has the correct labelling:
        # "Event leading to Ownership of B-A138 0002 (1774-05-30)"
        # but it doesn't seem to be linked anywhere. Where is it coming from?
        e3 = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PROV,Seller-0,OBJ,B-A138,0002,1774-05-30']
        self.assertEqual(e3['_label'], 'Event leading to Ownership of B-A138 0002 (1774-05-30)')

if __name__ == '__main__':
    unittest.main()
