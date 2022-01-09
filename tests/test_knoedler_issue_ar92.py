#!/usr/bin/env python3 -B
import unittest

from tests import TestKnoedlerPipelineOutput, classified_identifiers
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR92(TestKnoedlerPipelineOutput):
    def test_modeling_ar92(self):
        '''
        AR-92: Do not model payments' carried_out_by on top-level activity
        '''
        output = self.run_pipeline('ar92')
        lo = output['model-lo']
        activities = output['model-activity']
        objects = output['model-object']

        sale = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,Out,5,190,38']        
        parts = sale['part']
        payments = [p for p in parts if p['type'] == 'Payment']
        
        # assert that there is a payment
        self.assertEqual(len(payments), 1)
        payment = payments[0]
        self.assertEqual(payment['_label'], 'Payment for Stock Number 12024 (1910-07-25)')
        paym_parts = payment['part']
        
        # but the top-level payment does not have any carried_out_by data
        # (which data is instead represented by the paid_{from,to} properties)
        self.assertNotIn('carried_out_by', payment)


if __name__ == '__main__':
    unittest.main()
