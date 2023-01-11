#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR154(TestSalesPipelineOutput):
    def get_services_override(self):
        return {
            'problematic_records': {
                "lots": [
                    [["F-A791", "0069", "1784-06-21"], "multiple values may have been used in present_loc_inst: F-A791 0069 (1784-06-21)"]
                ]
            }
        }

    def test_modeling_ar154(self):
        '''
        AR-154: Update URI for problematic records
        '''
        output = self.run_pipeline('ar154')
        activities = output['model-sale-activity']
        
        auction = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION,F-A791,0069,1784-06-21']
        refs = auction.get('referred_to_by', [])
        problems = [r for r in refs if r.get('classified_as', [{}])[0].get('_label') == 'Problematic Record']
        self.assertEqual(len(problems), 1)
        problem = problems[0]
        cl = problem['classified_as'][0]
        self.assertEqual(cl['id'], 'https://data.getty.edu/local/thesaurus/problematic-record')


if __name__ == '__main__':
    unittest.main()




