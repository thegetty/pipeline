#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestKnoedlerPipelineOutput, classified_identifiers, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR38(TestKnoedlerPipelineOutput):
    '''
    AR-38: Knoedler stock numbers not correctly identified on physical object model
    '''
    def test_modeling_ar38(self):
        output = self.run_pipeline('ar38')

        objects = output['model-object']
        obj = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,2391']

        self.assertEqual(classified_identifiers(obj), {
            'Stock Number': '2391',
            'Title': 'Head of young girl',
        })
    
    '''
    AR-38: Check concordance sheet is aligned and add functionality to count the records merged using it
    '''
    def test_concordance_and_count_merged_ar38(self):
        output = self.run_pipeline('ar38')
        
        objects = output['model-object']
        obj1 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,1106']
        identifiers1 = classified_identifier_sets(obj1)
        self.assertEquals(len(identifiers1['Stock Number']), 4)
        
        obj2 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,11318']
        identifiers2 = classified_identifier_sets(obj2)
        self.assertEquals(len(identifiers2['Stock Number']), 2)
        
        obj3 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,2391']
        identifiers3 = classified_identifier_sets(obj3)
        self.assertEquals(len(identifiers3['Stock Number']), 1)
        
if __name__ == '__main__':
    unittest.main()
