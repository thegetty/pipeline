#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classification_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR119(TestSalesPipelineOutput):
    def test_modeling_ar119(self):
        '''
        AR-119: Get Rid of Bidding Model
        '''
        output = self.run_pipeline('ar119')
        activities = output['model-activity']
        self.assertNotIn('model-bidding', output)

        offer1 = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PROV,Br-4537,1836-01-30,0035']
        assignments1 = [p for p in offer1.get('part', []) if p['type'] == 'AttributeAssignment']
        self.assertEqual(len(assignments1), 1)
        assignment1 = assignments1[0]
        self.assertEqual(assignment1['_label'], 'Bidding valuation of Br-4537 0035 1836-01-30')
        self.assertEqual(classification_sets(assignment1), {'Bidding'})
        amounts1 = assignment1['assigned']
        self.assertEqual(len(amounts1), 1)
        amount1 = amounts1[0]
        self.assertEqual(amount1['_label'], '3.50 pounds')
        buyers1 = assignment1.get('carried_out_by', [])
        self.assertEqual(len(buyers1), 0) # bought-in, so no buyer data

        offer2 = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PROV,Br-4536,1836-01-29,0008']
        assignments2 = [p for p in offer2.get('part', []) if p['type'] == 'AttributeAssignment']
        self.assertEqual(len(assignments2), 1)
        assignment2 = assignments2[0]
        self.assertEqual(assignment2['_label'], 'Bidding valuation of Br-4536 0008 1836-01-29')
        self.assertEqual(classification_sets(assignment2), {'Bidding'})
        amounts2 = assignment2['assigned']
        self.assertEqual(len(amounts2), 1)
        amount2 = amounts2[0]
        self.assertEqual(amount2['_label'], '0.14 pounds')
        buyers2 = assignment2.get('carried_out_by', [])
        self.assertEqual(len(buyers2), 1)
        buyer2 = buyers2[0]
        self.assertEqual(buyer2['_label'], 'Balmer')


if __name__ == '__main__':
    unittest.main()
