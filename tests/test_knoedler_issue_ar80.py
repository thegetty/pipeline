#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestKnoedlerPipelineOutput, classified_identifier_sets, classification_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR80(TestKnoedlerPipelineOutput):
    def test_modeling_ar80(self):
        '''
        AR-80: Improve modeling of external links
        '''
        output = self.run_pipeline('ar80')
        texts = output['model-lo']
        
        record1 = texts['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Text,Book,2']
        
        self.verifyReferences(record1, {
            'http://hdl.handle.net/10020/2012m54_fl4155233': {
            	'classification': 'Digital Image',
            	'note': 'URL to image of stockbook page where object appears, hosted by Rosetta. Goes directly to hosted image, not a citation page.'
            }
        })

    def verifyReferences(self, record, expectedUrlsDict):
        expectedUrls = expectedUrlsDict.keys()
        self.assertIn('referred_to_by', record)
        refs = record['referred_to_by']
        self.assertEqual(len(refs), len(expectedUrls))
        
        got = {}
        import pprint
        for r in refs:
            reftype = r['type']
            self.assertEqual(len(r['access_point']), 1)
            url = r['access_point'][0]['id']
            got[url] = {
                'type': reftype,
                'refs': classified_identifier_sets(r, 'referred_to_by'),
                'classification': classification_sets(r)
            }
        
        for url, expected in expectedUrlsDict.items():
            gotData = got[url]
            if 'note' in expected:
                self.assertIn(expected['note'], gotData['refs']['Note'])
            if 'classification' in expected:
                self.assertIn(expected['classification'], gotData['classification'])


if __name__ == '__main__':
    unittest.main()
