#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestPeoplePipelineOutput, classified_identifiers

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR69(TestPeoplePipelineOutput):
    '''
    AR-69: Corporate body formation and dissolution
    '''
    def test_modeling_ar69(self):
        output = self.run_pipeline('ar69')

        groups = output['model-groups']
        group1 = groups['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Alexander%20City%2C%20AL%2C%20USA.%20%20Alexander%20City%20Public%20Library']
        group2 = groups['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,London%2C%20England%2C%20UK.%20%20Dulwich%20Picture%20Gallery']
        
        self.assertIn('formed_by', group1)
        self.assertIn('dissolved_by', group1)
        self.assertEqual(group1['formed_by']['timespan']['_label'], '1886')
        self.assertEqual(group1['dissolved_by']['timespan']['_label'], '1934')
        
        self.assertIn('formed_by', group2)
        self.assertNotIn('dissolved_by', group2)
        self.assertEqual(group2['formed_by']['timespan']['_label'], '1811')

if __name__ == '__main__':
    unittest.main()