#!/usr/bin/env python3 -B
import unittest

from tests import TestKnoedlerPipelineOutput, classification_sets
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR121(TestKnoedlerPipelineOutput):
    def test_modeling_ar121(self):
        '''
        AR-121: Populate the Subject Field of Textual Work
        '''
        output = self.run_pipeline('ar121')
        texts = output['model-lo']
        
        expected = {
			'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Text,Book,2,Page,222,Row,15': 'Lost',
			'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Text,Book,3,Page,11,Row,20': 'Inventorying',
			'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Text,Book,3,Page,13,Row,1': 'Returning',
			'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Text,Book,3,Page,78,Row,21': 'Gift',
			'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Text,Book,4,Page,197,Row,18': 'Exchange',
			'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Text,Book,4,Page,29,Row,14': 'Purchase',
			'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Text,Book,8,Page,215,Row,17': 'Destroyed',
			'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Text,Book,9,Page,33,Row,13': 'Stolen'
        }
        for url, cl in expected.items():
            record = texts[url]
            got = classification_sets(record, classification_key='about')
            self.assertIn(cl, got)


if __name__ == '__main__':
    unittest.main()
