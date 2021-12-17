#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestKnoedlerPipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR84(TestKnoedlerPipelineOutput):
    '''
    AR-84: Knoedler record enrichment
    '''
    def test_modeling_ar84(self):
        output = self.run_pipeline('ar84')
        texts = output['model-lo']

        page = texts['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Text,Book,5,Page,237']
        parts = page['part']
        
        # Assert that there are two parts to the page, but only one classification of 'Heading' (not 'SubHeading')
        self.assertEqual(len(parts), 2)
        cls = {p.get('classified_as', [{}])[0].get('_label') for p in parts}
        self.assertEqual(cls, {'Heading'})


if __name__ == '__main__':
    unittest.main()
