#!/usr/bin/env python3 -B
import unittest

from tests import TestKnoedlerPipelineOutput, classification_sets
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR132(TestKnoedlerPipelineOutput):
    def test_modeling_ar132(self):
        '''
        AR-132: Add transaction types on to Knoedler provenance activity records
        '''
        output = self.run_pipeline('ar132')
        activies = output['model-activity']
        
        expected = {
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,In,4,29,14': 'Purchase',
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,In,3,12,7' : 'Purchase',
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,In,3,13,1' : 'Purchase',
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,In,8,208,43' : 'Purchase',
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,In,8,215,17' : 'Purchase',
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,Out,3,11,20' : 'Inventorying',
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,Out,8,208,43' : 'Lost',
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,Out,4,29,14' : 'Purchase',
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,Out,3,13,1' : 'Sale (Return to Original Owner)',
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,Out,9,33,13' : 'Stolen'
        }

        for url,cl in expected.items():
            activity = activies[url]
            got = classification_sets(activity, classification_key='classified_as')
            self.assertIn(cl,got)

        # an unsold with sellers is not an inventorying event
        unsold = classification_sets(activies['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,In,3,12,7'])
        self.assertNotIn('Inventorying',unsold)


if __name__ == '__main__':
    unittest.main()
