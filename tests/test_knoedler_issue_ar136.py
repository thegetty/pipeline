#!/usr/bin/env python3 -B
import unittest

from tests import TestKnoedlerPipelineOutput, classification_sets
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR136(TestKnoedlerPipelineOutput):
    def test_modeling_ar136(self):
        '''
        AR-136: Remove modelling of physical object for pages in the Knoedler
        '''
        output = self.run_pipeline('ar136')
        texts = output['model-lo']
        objects = output['model-object']
        
        # for a given page, there is a LinguisticObject, but not a HumanMadeObject
        self.assertIn('tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Text,Book,8,Page,215', texts)
        self.assertNotIn('tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Book,8,Page,215', objects)


if __name__ == '__main__':
    unittest.main()
