#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR129(TestSalesPipelineOutput):
    def test_modeling_ar129(self):
        '''
        AR-129: Include original STAR data in textual work records
        '''
        output = self.run_pipeline('ar129')
        texts = output['model-lo']
        
        self.verifyContents(texts)
        self.verifyCatalogs(texts)
        self.verifyEvents(texts)
    
    def verifyContents(self, texts):
        # record for entry in sales_contents
        sale = texts['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,F-2,RECORD,805516']
        rows = sale['features_are_also_found_on']
        self.assertEqual(len(rows), 1)
        row = rows[0]
        
        self.assertIn('content', row)
        content = row['content']
        
        # set field
        self.assertIn('art_authority_1: LA RIVE, PIERRE LOUIS DE\n', content)
        
        # empty field
        self.assertIn('pg: \n', content)
    
    def verifyCatalogs(self, texts):
        # record for entry in sales_catalogs
        row = texts['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#ENTRY,PHYS-CAT,F-2,CCPP']
        self.assertIn('content', row)
        content = row['content']
        
        # set field
        self.assertIn('star_record_no: 10456\ncatalog_number: F-2\n', content)
        
        # empty field
        self.assertIn('copy_number: \n', content)

    def verifyEvents(self, texts):
        # record for entry in sales_descriptions
        catalog = texts['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,F-2']
        rows = catalog['features_are_also_found_on']
        self.assertEqual(len(rows), 1)
        row = rows[0]
        
        self.assertIn('content', row)
        content = row['content']
        
        # set field
        self.assertIn('star_record_no: 10461\ncatalog_number: F-2\n', content)
        
        # empty field
        self.assertIn('sale_end_month: \n', content)


if __name__ == '__main__':
    unittest.main()
