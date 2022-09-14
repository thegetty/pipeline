#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestKnoedlerPipelineOutput, classified_identifiers

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR146(TestKnoedlerPipelineOutput):
    '''
    AR-146: Add missing Provenance Activity Type Names for Lost/Stolen records
    '''
    def test_modeling_ar146(self):
        output = self.run_pipeline('ar146')

        provenance = output['model-activity']
        theft = provenance['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,Out,9,33,13']
        lost = provenance['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,Out,2,222,15']

        self.assertEqual(len(theft['identified_by']), 1)
        self.assertEqual(len(lost['identified_by']), 1)
        
        self.assertEqual(theft['identified_by'][0]['content'], 'Theft of Stock Number A2171')
        self.assertEqual(lost['identified_by'][0]['content'], 'Loss of [GRI Number K-15085]')
        

if __name__ == '__main__':
    unittest.main()
