#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestPeoplePipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR129(TestPeoplePipelineOutput):
    def test_modeling_ar129(self):
        '''
        AR-129: Include original STAR data in textual work records
        '''
        output = self.run_pipeline('ar129')
        texts = output['model-lo']
		
        entry = texts['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:people#ENTRY,PEOPLE,2']

        self.assertIn('content', entry)
        content = entry['content']
        
        # set fields
        self.assertIn('star_record_no: 2\nperson_authority: RUBENS, PETER PAUL\n', content)
        
        # empty field
        self.assertIn('corporate_body: \n', content)

if __name__ == '__main__':
    unittest.main()
