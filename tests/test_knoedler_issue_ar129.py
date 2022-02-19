#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestKnoedlerPipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR129(TestKnoedlerPipelineOutput):
    def test_modeling_ar129(self):
        '''
        AR-129: Include original STAR data in textual work records
        '''
        output = self.run_pipeline('ar129')
        texts = output['model-lo']
        
        entry = texts['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Text,Book,4,Page,201,Row,13']
        rows = entry['features_are_also_found_on']
        self.assertEqual(len(rows), 1)
        row = rows[0]

        self.assertIn('content', row)
        content = row['content']
        
        # set field
        self.assertIn('star_record_no: 50445\n', content)
        
        # empty field
        self.assertIn('consign_loc: \n', content)

if __name__ == '__main__':
    unittest.main()
