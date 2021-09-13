#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifiers

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR40(TestSalesPipelineOutput):
    def test_modeling_ar40(self):
        '''
        AR-40: Associate Production to sale row record for attribution modifiers
        '''
        output = self.run_pipeline('ar40')
        objects = output['model-object']
        
        obj1 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,B-213,0005,1813-02-22']
        prod = obj1['produced_by']
        attrs = prod['attributed_by']

		# assert that attribute assignments on the Production have a reference
		# to the sales record (entry in the sale catalog)
        self.assertEqual(len(attrs), 2)
        for attr in attrs:
            self.assertIn('used_specific_object', attr)
            used_objs = attr['used_specific_object']
            self.assertEqual(len(used_objs), 1)
            used = used_objs[0]
            self.assertIn('Sale recorded in catalog: B-213 0005 (1813-02-22)', used['_label'])


if __name__ == '__main__':
    unittest.main()
