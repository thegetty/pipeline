#!/usr/bin/env python3 -B
import unittest

from tests import TestKnoedlerPipelineOutput, classification_sets
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR40(TestKnoedlerPipelineOutput):
    def test_modeling_ar40(self):
        '''
        AR-40: Attribution for Production (attribution modifiers) not associated to row record that supports attribution
        '''
        output = self.run_pipeline('ar40')
        objects = output['model-object']

        # "possibly by"
        hmo = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,Internal,K-16119']
        self.verifyObjectAttribution(hmo, 'possibly by')

        # "attributed to"
        hmo = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,7843']
        self.verifyObjectAttribution(hmo, 'attributed to')
        
        # "style of; manner of"
        hmo = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,13556']
        self.verifyObjectAttribution(hmo, 'style of; manner of')

        # "style of; manner of"
        hmo = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,13006']
        self.verifyObjectAttribution(hmo, 'attributed to; school of')

    def verifyObjectAttribution(self, hmo, verbatim_attrib_mod):
        prod = hmo['produced_by']
        self.assertIn('attributed_by', prod)
        self.assertEqual(len(prod['attributed_by']), 1)
        attr = prod['attributed_by'][0]

        self.assertIn('carried_out_by', attr)
        self.assertIn('referred_to_by', attr)
        
        carried_out_by = attr['carried_out_by']
        referred_to_by = attr['referred_to_by']
        self.assertEqual(len(referred_to_by), 1)
        self.assertEqual(len(carried_out_by), 1)

        self.assertEqual(referred_to_by[0]['content'], verbatim_attrib_mod)
        self.assertEqual(carried_out_by[0]['_label'], 'M. Knoedler & Co.')


if __name__ == '__main__':
    unittest.main()
