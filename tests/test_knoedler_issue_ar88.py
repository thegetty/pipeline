#!/usr/bin/env python3 -B
import unittest

from tests import TestKnoedlerPipelineOutput, classified_identifiers
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR88(TestKnoedlerPipelineOutput):
    def test_modeling_ar88(self):
        '''
        AR-88: Update modeling of Knoedler records to use book/page/row number identifiers
        '''
        output = self.run_pipeline('ar88')
        activities = output['model-activity']
        
        purch = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,In,5,190,38']
        parts = purch['part']
        rightsacqs = [p for p in parts if p['type'] == 'RightAcquisition']
        self.assertEquals(len(rightsacqs), 1)

        rights = rightsacqs[0]['establishes']
        parts = rights['part']
        self.assertEquals(len(parts), 3)
        
        for r in parts:
        	dims = r['dimension']
	        self.assertEquals(len(dims), 1)
	        dim = dims[0]
	        self.assertEqual(dim['value'], '33.33')


if __name__ == '__main__':
    unittest.main()
