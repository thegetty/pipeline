#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestPeoplePipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR100(TestPeoplePipelineOutput):
    '''
    AR-100: Change the Name type for groups from 'Personal Names' to 'Corporate Names'
    '''
    def test_modeling_ar100(self):
        output = self.run_pipeline('ar100')
        groups = output['model-groups']
        
        germans = groups['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#GROUP,AUTH,%5BGERMAN%20-%2014TH%20C.%5D']
        gallery = groups['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Edinburgh%2C%20Scotland%2C%20UK.%20%20Scottish%20National%20Portrait%20Gallery']
        
        # Assert that a Gallery has a "Corporate Name" and a "Display Title"
        self.assertIn('Corporate Name', classified_identifier_sets(gallery))
        self.assertIn('Display Title', classified_identifier_sets(gallery))
        self.assertEqual(classified_identifier_sets(gallery), {
			'Corporate Name': {
				'Edinburgh, Scotland, UK.  Scottish National Portrait Gallery',
				'National Portrait Gallery',
				'Scottish National Portrait Gallery'
			},
			'Display Title': {'Scottish National Portrait Gallery'},
			'Primary Name': {'Edinburgh, Scotland, UK.  Scottish National Portrait Gallery'},
			'STAR Identifier': {'61833'}
		})
        
        # and that a generic group representing people also uses "Corporate Name"
        self.assertIn('Corporate Name', classified_identifier_sets(germans))


if __name__ == '__main__':
    unittest.main()
