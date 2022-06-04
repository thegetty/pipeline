#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classification_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR120(TestSalesPipelineOutput):
    def test_modeling_ar120(self):
        '''
        AR-120: Add bid records when no price indicated in a sales record.
        '''
        output = self.run_pipeline('ar120')
        texts = output['model-lo']
        activities = output['model-activity']
        
        prov1 = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PROV,B-A15,1739-07-20,0233']
        assignments1 = [p for p in prov1.get('part', []) if p['type'] == 'AttributeAssignment']
        self.assertEqual(len(assignments1), 1)
        assignment1 = assignments1[0]
        self.assertEqual(assignment1['_label'], 'Bidding valuation of B-A15 0233 1739-07-20')
        self.assertEqual(classification_sets(assignment1), {'Bidding'})
        self.assertNotIn('assigned', assignment1)
        buyers1 = assignment1.get('carried_out_by', [])
        self.assertEqual(len(buyers1), 1) # bought-in, so no buyer data
        buyer1 = buyers1[0]
        self.assertEqual(buyer1['_label'], 'Servees')

        prov2 = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PROV,Br-A1875,1792-02-18,0008']
        assignments2 = [p for p in prov2.get('part', []) if p['type'] == 'AttributeAssignment']
        self.assertEqual(len(assignments2), 1)
        assignment2 = assignments2[0]
        self.assertEqual(assignment2['_label'], 'Bidding valuation of Br-A1875 0008 1792-02-18')
        self.assertEqual(classification_sets(assignment2), {'Bidding'})
        self.assertNotIn('assigned', assignment2)
        buyers2 = assignment2.get('carried_out_by', [])
        self.assertEqual(len(buyers2), 1) # bought-in, so no buyer data
        buyer2 = buyers2[0]
        self.assertEqual(buyer2['_label'], 'Skirrow')


if __name__ == '__main__':
    unittest.main()
