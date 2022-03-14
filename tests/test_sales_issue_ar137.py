#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR137(TestSalesPipelineOutput):
    def test_modeling_ar137(self):
        '''
        AR-137: Change name of Records in Collection / Set Model
        '''
        output = self.run_pipeline('ar137')
        sets = output['model-set']

        set1 = sets['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION,D-B2514,0088,1914-04-02-Set']
        self.assertEqual(set1['_label'], 'Lot D-B2514 0088')

        set2 = sets['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION,D-3008,0106,1944-02-22-Set']
        self.assertEqual(set2['_label'], 'Lot D-3008 0106')
        
        set3 = sets['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION,F-A1039,0001,1793-01-Set']
        self.assertEqual(set3['_label'], 'Lot F-A1039 0001')
        
        set4 = sets['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION,D-A37,0008,1763-Set']
        self.assertEqual(set4['_label'], 'Object Set D-A37 0008')
        
        set5 = sets['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION,Br-1159,1597,1814-Set']
        self.assertEqual(set5['_label'], 'Object Set Br-1159 1597')


if __name__ == '__main__':
    unittest.main()
