#!/usr/bin/env python3 -B
import unittest

from tests import TestSalesPipelineOutput, classification_sets
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR139(TestSalesPipelineOutput):
    def test_modeling_ar139(self):
        '''
        AR-139: For all Textual Work Records in Sales and Knoedler make sure that each record has both a form and a content type
        '''
        output = self.run_pipeline('ar139')
        texts = output['model-lo']
        
        print(texts)
        catalog = texts['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,D-2748']
        page = texts['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,D-2748,Page,11']
        entry = texts['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,D-2748,RECORD,1052503']

        # all textual works should inherit catalog's type in Sales (type of this example that gets inherited: auction catalogs)
        self.assertEqual(classification_sets(catalog, key='id'), {'http://vocab.getty.edu/aat/300026068','http://vocab.getty.edu/aat/300026059'})
        self.assertEqual(classification_sets(page, key='id'), {'http://vocab.getty.edu/aat/300026068','http://vocab.getty.edu/aat/300194222'})
        self.assertEqual(classification_sets(entry, key='id'), {'http://vocab.getty.edu/aat/300026068','http://vocab.getty.edu/aat/300438434'})



if __name__ == '__main__':
    unittest.main()
