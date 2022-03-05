#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR80(TestSalesPipelineOutput):
    def test_modeling_ar80(self):
        '''
        AR-80: Improve modeling of external links
        '''
        output = self.run_pipeline('ar80')
        texts = output['model-lo']
        
        record1 = texts['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,N-A13']
        record2 = texts['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,Br-A348']
        record3 = texts['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,D-A51']
        record4 = texts['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,SC-A38']

        self.verifyReferences(record1, {
            'http://dx.doi.org/10.1163/2210-7886_ASC-162',      # art_sales_cats_online
            'http://archive.org/details/catalogusofnaaml01hoet' # link_to_PDF
        })

        self.verifyReferences(record2, {
            'http://artworld.york.ac.uk/'                       # art_world_in_britain
        })

        self.verifyReferences(record3, {
            'http://dx.doi.org/10.1163/2210-7886_ASC-1490',     # art_sales_cats_online
            'http://portal.getty.edu/books/inha_17892'          # portal_url_1
        })

        self.verifyReferences(record4, {
            'http://dx.doi.org/10.1163/2210-7886_ASC-3529'      # art_sales_cats_online
        })

    def verifyReferences(self, record, expectedUrls):
        self.assertIn('referred_to_by', record)
        refs = record['referred_to_by']
        self.assertEqual(len(refs), len(expectedUrls))
        types = {r['type'] for r in refs}
        self.assertEqual(types, {'DigitalObject'})
        urls = {r['id'] for r in refs}
        self.assertEqual(urls, expectedUrls)


if __name__ == '__main__':
    unittest.main()
