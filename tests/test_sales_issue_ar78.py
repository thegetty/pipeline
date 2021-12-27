#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR78(TestSalesPipelineOutput):
    def test_modeling_ar78(self):
        '''
        AR-78: Add translated titles for Scandinavian reords
        '''
        output = self.run_pipeline('ar78')
        objects = output['model-object']
        
        obj1 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,SC-A68,0149,1796-04-20']
        self.assertEqual(classified_identifier_sets(obj1), {
        	'Lot Number': {'0149'},
        	'Primary Name': {'Jagtstykke med en Hiort'},
        	'Title': {
				'2de Jagtstykker, det ene med en Hiort, det andet med en Löve [Malerier i Oliefarve]',
				'Hunting piece with a deer',
				'Jagtstykke med en Hiort'
			},
			'Translated Title': {'Hunting piece with a deer'}
        })
        
        idents = [i for i in obj1['identified_by'] if i['content'] == 'Hunting piece with a deer']
        self.assertEqual(len(idents), 1)
        i = idents[0]
        self.assertIn('language', i)
        self.assertEqual(i['language'][0]['_label'], 'English')

if __name__ == '__main__':
    unittest.main()
