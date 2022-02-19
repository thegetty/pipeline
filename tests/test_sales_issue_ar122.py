#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR122(TestSalesPipelineOutput):
    def test_modeling_ar122(self):
        '''
        AR-122: Remove modeling of destruction events
        '''
        output = self.run_pipeline('ar122')
        objects = output['model-object']
        
        obj1 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-769,0068,1810-05-24']
        obj2 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-991,0099,1812-05-23']

        for obj in objects.values():
            self.assertNotIn('destroyed_by', obj)

if __name__ == '__main__':
    unittest.main()
