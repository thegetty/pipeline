#!/usr/bin/env python3 -B
import unittest

from tests import TestKnoedlerPipelineOutput, classified_identifier_sets
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR101(TestKnoedlerPipelineOutput):
    def test_modeling_ar101(self):
        '''
        AR-101: Replace Repository Number Type with Star Identifier Type
        '''
        output = self.run_pipeline('ar101')
        lo = output['model-lo']
        catalog = lo['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Text,Book,5,Page,190,Row,38']
        
        self.assertEqual(classified_identifier_sets(catalog), {
			'Entry Number': {'38'},
			'STAR Identifier': {'53051'},
			'Title': {'Knoedler Stock Book 5, Page 190, Row 38'}
		})


if __name__ == '__main__':
    unittest.main()
