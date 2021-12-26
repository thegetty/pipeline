#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestPeoplePipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR101(TestPeoplePipelineOutput):
    '''
    AR-101: Replace Repository Number Type with Star Identifier Type
    '''
    def test_modeling_ar101(self):
        output = self.run_pipeline('ar101')
        groups = output['model-groups']
        people = output['model-person']
        
        group = groups['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Boston%2C%20MA%2C%20USA.%20%20Isabella%20Stewart%20Gardner%20Museum']
        self.assertEqual(classified_identifier_sets(group), {
            None: {'Isabella Stewart Gardner Museum'},
            'Personal Name': {'Boston, MA, USA.  Isabella Stewart Gardner Museum',
            'Boston, MA.  Isabella Stewart Gardner Museum'},
            'Primary Name': {'Boston, MA, USA.  Isabella Stewart Gardner Museum'},
            'STAR Identifier': {'31556'}
        })
        
        person = people['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,MONOGRAMMIST%20F.Y.']
        self.assertEqual(classified_identifier_sets(person), {
        	'Personal Name': {'F. Y.', 'Monogrammist F.Y.', 'F.Y.'},
			'Primary Name': {'MONOGRAMMIST F.Y.'},
			'STAR Identifier': {'13590'}}
		)

if __name__ == '__main__':
    unittest.main()