#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestKnoedlerPipelineOutput, classified_identifiers

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR42(TestKnoedlerPipelineOutput):
    '''
    AR-42: Use PLOC Data to Populate Current Location Field
    '''
    def test_modeling_ar42(self):
        output = self.run_pipeline('ar42')

        objects = output['model-object']
        obj = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,15649']

        self.assertIn('current_location', obj)
        self.assertEqual(obj['current_location']['_label'], 'Melbourne, Victoria, Australia')

        self.assertIn('current_owner', obj)
        self.assertEqual(len(obj['current_owner']), 1)
        self.assertEqual(obj['current_owner'][0]['_label'], 'National Gallery of Victoria (Melbourne, Victoria, Australia)')

if __name__ == '__main__':
    unittest.main()



