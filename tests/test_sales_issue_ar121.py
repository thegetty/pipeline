#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classification_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR121(TestSalesPipelineOutput):
    def test_modeling_ar121(self):
        '''
        AR-121: Populate the Subject Field of Textual Work
        '''
        output = self.run_pipeline('ar121')
        texts = output['model-lo']
        
        expected = {
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,D-A92,RECORD,936865': 'Bought In',
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,B-A111,RECORD,12666': 'Bought In',
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,B-A13,RECORD,7215': 'Withdrawn',
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,B-A136,RECORD,1': None,
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,B-A138,RECORD,138': 'Purchase',
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,Br-4716,RECORD,105972': None,
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,D-A63,RECORD,935519': None,
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,D-A63,RECORD,935520': 'Purchase',
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,D-A92,RECORD,936865': 'Bought In',
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,D-A96,RECORD,937738': 'Withdrawn',
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,F-A1045,RECORD,716405': 'Bought In',
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,F-A12,RECORD,716158': 'Withdrawn',
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,F-A458,RECORD,715256': None,
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,F-A458,RECORD,715265': 'Purchase'
        }
        for url, cl in expected.items():
            record = texts[url]
            got = classification_sets(record, classification_key='about')
            self.assertIn(cl, got)


if __name__ == '__main__':
    unittest.main()
