#!/usr/bin/env python3 -B
import unittest

from tests import TestKnoedlerPipelineOutput, classification_sets, classification_tree
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR102(TestKnoedlerPipelineOutput):
    def test_modeling_ar102(self):
        '''
        AR-102: Add form types to textual work records
        '''
        output = self.run_pipeline('ar102')
        texts = output['model-lo']
        
        book = texts['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Text,Book,5']
        page = texts['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Text,Book,5,Page,190']
        entry = texts['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Text,Book,5,Page,190,Row,38']
        
        self.assertEqual(classification_sets(book, key='id'), {'http://vocab.getty.edu/aat/300028051', 'http://vocab.getty.edu/aat/300027483'})
        self.assertTrue(classification_sets(page, key='id').issuperset({'http://vocab.getty.edu/aat/300194222'}))
        self.assertTrue(classification_sets(entry, key='id').issuperset({'http://vocab.getty.edu/aat/300438434'}))
        
        self.assertEqual(classification_tree(book, key='id'), {
        	'http://vocab.getty.edu/aat/300027483': {'http://vocab.getty.edu/aat/300435443': {}}, # Account Book => Type of Work
			'http://vocab.getty.edu/aat/300028051': {'http://vocab.getty.edu/aat/300444970': {}}  # Book => Form
		})
        self.assertDictContainsSubset({'http://vocab.getty.edu/aat/300194222': {'http://vocab.getty.edu/aat/300444970': {}}  # Page => Form
        }, classification_tree(page, key='id'))
        self.assertDictContainsSubset({'http://vocab.getty.edu/aat/300438434': {'http://vocab.getty.edu/aat/300444970': {}}  # Entry => Form
        }, classification_tree(entry, key='id'))


if __name__ == '__main__':
    unittest.main()
