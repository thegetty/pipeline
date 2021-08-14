#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestKnoedlerPipelineOutput, classified_identifiers

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR38(TestKnoedlerPipelineOutput):
    '''
    AR-38: Knoedler stock numbers not correctly identified on physical object model
    '''
    def test_modeling_ar38(self):
        output = self.run_pipeline('ar38')

        objects = output['model-object']
        obj = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,2391']

        self.assertEqual(classified_identifiers(obj), {
            'Stock Number': '2391',
            'Title': 'Head of young girl',
        })

if __name__ == '__main__':
    unittest.main()
