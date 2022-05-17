#!/usr/bin/env python3 -B
import unittest

from tests import TestKnoedlerPipelineOutput, classification_sets
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR139(TestKnoedlerPipelineOutput):
    def test_modeling_ar139(self):
        '''
        AR-139: For all Textual Work Records in Sales and Knoedler make sure that each record has both a form and a content type
        '''
        output = self.run_pipeline('ar139')
        texts = output['model-lo']
        
        book = texts['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Text,Book,5']
        page = texts['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Text,Book,5,Page,190']
        entry = texts['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Text,Book,5,Page,190,Row,38']

        # all textual works should inherit book's type account books in Knoedler
        self.assertEqual(classification_sets(book, key='id'), {'http://vocab.getty.edu/aat/300027483','http://vocab.getty.edu/aat/300028051'})
        self.assertEqual(classification_sets(page, key='id'), {'http://vocab.getty.edu/aat/300027483','http://vocab.getty.edu/aat/300194222'})
        self.assertEqual(classification_sets(entry, key='id'), {'http://vocab.getty.edu/aat/300027483','http://vocab.getty.edu/aat/300438434'})



if __name__ == '__main__':
    unittest.main()
