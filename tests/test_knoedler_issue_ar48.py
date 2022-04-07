#!/usr/bin/env python3 -B
import unittest

from tests import TestKnoedlerPipelineOutput, classified_identifiers
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR48(TestKnoedlerPipelineOutput):
    def test_modeling_ar48(self):
        '''
        AR-48: Update modeling of Knoedler records to use book/page/row number identifiers
        '''
        output = self.run_pipeline('ar48')
        lo = output['model-lo']
        objects = output['model-object']
        
        phys_book = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Book,3']

        text_book = lo['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Text,Book,3']
        text_page = lo['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Text,Book,3,Page,20']
        text_row = lo['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Text,Book,3,Page,20,Row,42']

		# Test the physical object records
        self.assertDictContainsSubset({
            'Title': 'Knoedler Stock Book 3',
            'Book Number': '3'
        }, classified_identifiers(phys_book))

		# Test the linguistic object records
        self.assertDictContainsSubset({
            'Title': 'Knoedler Stock Book 3',
            'Book Number': '3',
        }, classified_identifiers(text_book))
        self.assertDictContainsSubset({
            'Title': 'Knoedler Stock Book 3, Page 20',
            'Page Number': '20',
        }, classified_identifiers(text_page))
        self.assertDictContainsSubset({
            'Title': 'Knoedler Stock Book 3, Page 20, Row 42',
            'Entry Number': '42',
            'STAR Identifier': '44301',
        }, classified_identifiers(text_row))


if __name__ == '__main__':
    unittest.main()
