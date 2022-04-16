#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classification_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR123(TestSalesPipelineOutput):
    def test_modeling_ar123(self):
        '''
        AR-123: Record winning bid for Sold records
        '''
        output = self.run_pipeline('ar123')
        texts = output['model-lo']
        activities = output['model-activity']
        
        prov1 = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PROV,B-267,1817,0001']
        assignments1 = [p for p in prov1.get('part', []) if p['type'] == 'AttributeAssignment']
        self.assertEqual(len(assignments1), 1)
        assignment1 = assignments1[0]
        self.assertEqual(assignment1['_label'], 'Bidding valuation of B-267 0001 1817')
        self.assertEqual(classification_sets(assignment1), {'Bidding'})
        amounts1 = assignment1['assigned']
        self.assertEqual(len(amounts1), 1)
        amount1 = amounts1[0]
        self.assertEqual(amount1['_label'], '50,000.00 frs')
        self.assertEqual(classification_sets(amount1), {'Sale Price'})
        buyers1 = assignment1.get('carried_out_by', [])
        self.assertEqual(len(buyers1), 1)
        buyer1 = buyers1[0]
        self.assertEqual(buyer1['_label'], "Stier d'Aertselaer, Henri-Joseph, baron")
		
        prov2 = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PROV,B-3,1801-03-26,0045']
        assignments2 = [p for p in prov2.get('part', []) if p['type'] == 'AttributeAssignment']
        self.assertEqual(len(assignments2), 1)
        assignment2 = assignments2[0]
        self.assertEqual(assignment2['_label'], 'Bidding valuation of B-3 0045 1801-03-26')
        self.assertEqual(classification_sets(assignment2), {'Bidding'})
        amounts2 = assignment2['assigned']
        self.assertEqual(len(amounts2), 1)
        amount2 = amounts2[0]
        self.assertEqual(amount2['_label'], '0.10')
        self.assertEqual(classification_sets(amount2), {'Hammer Price'})
        buyers2 = assignment2.get('carried_out_by', [])
        self.assertEqual(len(buyers2), 1)
        buyer2 = buyers2[0]
        self.assertEqual(buyer2['_label'], 'Schell (Gent)')


if __name__ == '__main__':
    unittest.main()
