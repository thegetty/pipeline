#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestPeoplePipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR59(TestPeoplePipelineOutput):
    '''
    AR-59: Group Model Name Being Populated Incorrectly
    '''
    def test_modeling_ar59(self):
        output = self.run_pipeline('ar59')

        groups = output['model-groups']
        group = groups['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Philadelphia%2C%20PA%2C%20USA.%20%20Art%20Association%2C%20Union%20League%20Club']
        self.assertEqual(classified_identifier_sets(group), 		{
			None: {'Art Association, Union League Club'},
			'Primary Name': {'Philadelphia, PA, USA.  Art Association, Union League Club'},
			'Personal Name': {
				'Art Ass.n Union League Club',
				'Art Asson Union League Club',
				'Art Club Philadelphia',
				'Philadelphia, PA, USA.  Art Association, Union League Club',
				'U.L. Art Assn.',
				'Union League',
				'Union League Phila',
				'art assn phila'
			},
			'STAR-assigned Number': {'46175'}
		})


if __name__ == '__main__':
    unittest.main()
