#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR118(TestSalesPipelineOutput):
    def test_modeling_ar118(self):
        '''
        AR-118: Apply Modelling used buyer modifiers
        '''
        output = self.run_pipeline('ar115')
        activities = output['model-activity']
        
        prov = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PROV,Seller-0,OBJ,B-A138,0002,1774-05-30']
        custody_transfers = [p for p in prov.get('part', []) if p['type'] == 'TransferOfCustody']
        self.assertEqual(len(custody_transfers), 1)


if __name__ == '__main__':
    unittest.main()
