#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifiers, classification_tree

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR72(TestSalesPipelineOutput):
    def test_modeling_ar72(self):
        '''
        AR-72: Structure for textual work records in German sales
        '''
        output = self.run_pipeline('ar72')
        texts = output['model-lo']
        
        entry = texts['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,D-1310,RECORD,1216255']
        self.assertEqual(classification_tree(entry), {'Entry': {'Form': {}}})
        self.assertEqual(entry['_label'], 'Sale recorded in catalog: D-1310 0901 (1934-05-28) (record number 1216255)')
        entry_parents = entry['part_of']
        self.assertEqual(len(entry_parents), 1)
        entry_parent = entry_parents[0]
        self.assertIn(entry_parent['id'], texts)
        page = texts[entry_parent['id']]

        self.assertEqual(classification_tree(page), {'Page': {'Form': {}}})
        self.assertEqual(page['_label'], 'Sale Catalog D-1310, Page 60')
        page_parents = page['part_of']
        self.assertEqual(len(page_parents), 1)
        page_parent = page_parents[0]
        self.assertIn(page_parent['id'], texts)
        catalog = texts[page_parent['id']]

        self.assertEqual(classification_tree(catalog), {'Auction Catalog': {'Type of Work': {}}, 'Catalog': {'Form': {}}})
        self.assertEqual(catalog['_label'], 'Sale Catalog D-1310')


if __name__ == '__main__':
    unittest.main()
