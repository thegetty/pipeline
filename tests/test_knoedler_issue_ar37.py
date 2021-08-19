#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestKnoedlerPipelineOutput, classified_identifiers

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR37(TestKnoedlerPipelineOutput):
    '''
    AR-37: Instead of K numbers display the stock number for Knoedler resources
    '''
    def test_modeling_ar37(self):
        output = self.run_pipeline('ar37')
        activities = output['model-activity']
        
        # Purchase
        purchase = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,In,3,110,38']
        self.assertEqual(classified_identifiers(purchase), {
            None: 'Knoedler Purchase of Stock Number 2391 (1880)'
        })
        self.assertNotIn('K-1966', purchase['_label'])

        # Sale
        sale = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,Out,3,110,38']
        self.assertEqual(classified_identifiers(sale), {
            None: 'Knoedler Sale of Stock Number 2391 (1880-05-14)'
        })
        self.assertNotIn('K-1966', sale['_label'])

        # Inventorying
        inv = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,Out,3,53,2']
        self.assertEqual(classified_identifiers(inv), {
            None: 'Knoedler Inventorying of Stock Number 1228 (1878-08-14)'
        })
        self.assertNotIn('K-547', inv['_label'])


if __name__ == '__main__':
    unittest.main()
