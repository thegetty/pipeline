#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classification_sets


vocab.add_attribute_assignment_check()

class PIRModelingTest_AR132(TestSalesPipelineOutput):
    def test_modeling_ar132(self):
        '''
        AR-132: Add transaction types on to Sales provenance activity records
        '''
        output = self.run_pipeline('ar132')
        activies = output['model-activity']
        
        expected = {
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PROV,B-183,1810-10-26,0050' : 'Purchase',
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PROV,B-183,1810-10-26,0035' : 'Bought In'
        }

        expected_id = {
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PROV,B-183,1810-10-26,0005' : 'http://vocab.getty.edu/aat/300445698', # passed has no label
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PROV,B-183,1810-10-26,0015' : 'https://data.getty.edu/local/thesaurus/unknown', #unknown has no label
        }

        for url,cl in expected.items():
            activity = activies[url]
            got = classification_sets(activity, classification_key='classified_as')
            self.assertIn(cl,got)

        for url,cl in expected_id.items():
            activity = activies[url]
            got = classification_sets(activity, key="id", classification_key='classified_as')
            self.assertIn(cl,got)
        


if __name__ == '__main__':
    unittest.main()
