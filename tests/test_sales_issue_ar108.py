#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifiers

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR108(TestSalesPipelineOutput):
    def test_modeling_ar108(self):
        '''
        AR-108: Model auction house as actor of a sub-activity
        '''
        output = self.run_pipeline('ar108')
        activities = output['model-sale-activity']
        
        event = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION-EVENT,B-183']
        sale = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION,B-183,0050,1810-10-26']
        
        # The sale of a lot has the auction house represented as 'carried_out_by'
        self.assertIn('carried_out_by', sale)
        sale_actors = sale['carried_out_by']
        self.assertEqual(len(sale_actors), 1)
        sale_actor = sale_actors[0]
        self.assertEqual(sale_actor['_label'], 'Terlinck (E.J.)')
        
        # ... but the auction event does not
        self.assertNotIn('carried_out_by', event)
        
        # instead, it has the auction house represented as a sub-activity
        self.assertIn('part', event)
        parts = event['part']
        self.assertEqual(len(parts), 1)
        part = parts[0]
        self.assertEqual(part['_label'], 'Activity of Terlinck (E.J.)')
        
        self.assertIn('carried_out_by', part)
        event_actors = part['carried_out_by']
        self.assertEqual(len(event_actors), 1)
        event_actor = event_actors[0]
        self.assertEqual(event_actor['_label'], 'Terlinck (E.J.)')


if __name__ == '__main__':
    unittest.main()
