#!/usr/bin/env python3 -B
import unittest

from tests import TestKnoedlerPipelineOutput, classification_sets
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR253(TestKnoedlerPipelineOutput):
    def test_modeling_assert_obj_attr_assingees_get_merged_correctly(self):
        '''
        AR-253: Physical Object Model Errors
        '''
        output = self.run_pipeline('ar253')
        objects = output['model-object']
        obj = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,A3695']
        
        attr = [identifier for identifier in obj.get('identified_by', []) if 'http://vocab.getty.edu/aat/300312355' in classification_sets(identifier, key='id')]
        self.assertEqual(len([attr]), 1)
        self.assertEqual(len(attr[0]['assigned_by']), 1)
        self.assertEqual(len(attr[0]['assigned_by'][0]['carried_out_by']), 1)

if __name__ == '__main__':
    unittest.main()
