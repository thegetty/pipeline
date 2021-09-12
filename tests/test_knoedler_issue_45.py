#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestKnoedlerPipelineOutput, classified_identifiers

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR45(TestKnoedlerPipelineOutput):
    '''
    AR-45: Add references for physical object titles.
    '''
    def test_modeling_ar45(self):
        output = self.run_pipeline('ar45')

        objects = output['model-object']
        obj = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,14490']

        idents = [i for i in obj['identified_by'] if i['content'] == 'Port. of R. Gonzaga']
        self.assertEqual(len(idents), 1)
        ident = idents[0]
        
		# ensure that the object's title is referred to by the sale record (row in the Knoedler stock book)
        self.assertIn('referred_to_by', ident)
        refs = ident['referred_to_by']
        self.assertEqual(len(refs), 1)
        ref = refs[0]
        self.assertEqual(ref['_label'], 'Knoedler Stock Book 6, Page 177, Row 3')


if __name__ == '__main__':
    unittest.main()
