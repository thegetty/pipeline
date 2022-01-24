#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR77(TestSalesPipelineOutput):
    def test_modeling_ar77(self):
        '''
        AR-77: Add citation references for sales artist info
        '''
        output = self.run_pipeline('ar77')
        people = output['model-person']
        
        # This sales record has an artist_info field of "1795-1818".
        # This will get modeled as a Biography Statement which needs to have
        # a proper 'referred_to_by' citation link back to the sales catalog.
        person = people['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,FOHR%2C%20CARL%20PHILIPP']
        los = [l for l in person['referred_to_by'] if l.get('content') == '1795-1818']
        self.assertEqual(len(los), 1)
        lo = los[0]
        self.assertIn('referred_to_by', lo)
        refs = lo['referred_to_by']
        self.assertEqual(len(refs), 1)
        ref = refs[0]
        self.assertIn('D-1310 0901', ref['_label'])


if __name__ == '__main__':
    unittest.main()
