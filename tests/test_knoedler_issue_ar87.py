#!/usr/bin/env python3 -B
import unittest

from tests import TestKnoedlerPipelineOutput, classified_identifiers
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR87(TestKnoedlerPipelineOutput):
    def test_modeling_ar87(self):
        '''
        AR-87: Do not infer partial payment amounds from fractional share data.
        '''
        output = self.run_pipeline('ar87')
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
        
        # and that the payment is to 3 different recipients
        self.assertEqual(len(payment['paid_to']), 3)
        # but that there is only one known part of the payment
        self.assertEqual(len(paym_parts), 1)
        # that is TO Knoedler
        self.assertEqual(paym_parts[0]['_label'], 'M. Knoedler & Co. share of payment for Stock Number 12024 (1910-07-25)')
        paym_part = paym_parts[0]
        # in the amount of 5,333.60 pounds
        self.assertEqual(paym_part['paid_amount']['_label'], '5,333.60 pounds')


if __name__ == '__main__':
    unittest.main()
