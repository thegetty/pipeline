#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR128(TestSalesPipelineOutput):
    def test_modeling_ar128(self):
        '''
        AR-128: Link prov entry to object set
        '''
        output = self.run_pipeline('ar128')
        activities = output['model-activity']
        
        tx = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PROV,B-A136,1773-07-20,0090']
        self.assertIn('used_specific_object', tx)
        obj_sets = tx['used_specific_object']
        self.assertEquals(len(obj_sets), 1)
        obj_set = obj_sets[0]
        self.assertEquals(obj_set['_label'], 'Lot B-A136 0090')


if __name__ == '__main__':
    unittest.main()
