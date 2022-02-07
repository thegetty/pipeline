#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR79(TestSalesPipelineOutput):
    def test_modeling_ar79(self):
        '''
        AR-79: More aggressive merging on Place entities
        '''
        output = self.run_pipeline('ar79')
        places = output['model-place']
        london_records = 0
        for p in places.values():
        	name = p['_label']
        	if 'London' in name:
        		london_records += 1
        self.assertEqual(london_records, 1)


if __name__ == '__main__':
    unittest.main()
