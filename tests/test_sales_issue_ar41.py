#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifiers

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR41(TestSalesPipelineOutput):
    def test_modeling_ar41(self):
        '''
        AR-41: Stub Records generated in ETL for "copy after" are missing title
        '''
        output = self.run_pipeline('ar41')
        objects = output['model-object']

        obj = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,B-213,0005,1813-02-22-Original']
        self.assertEquals(classified_identifiers(obj), {
        	None: 'Unidentified by RUBENS, PETER PAUL'
        })
        
        production = obj['produced_by']
        parts = production['part']
        self.assertEquals(len(parts), 1)
        part = parts[0]
        artists = part['carried_out_by']
        self.assertEquals(len(artists), 1)
        artist = artists[0]
        self.assertEquals(artist['_label'], 'RUBENS, PETER PAUL')
        

if __name__ == '__main__':
    unittest.main()
