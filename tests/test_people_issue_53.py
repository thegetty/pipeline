#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestPeoplePipelineOutput, classified_identifiers

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR53(TestPeoplePipelineOutput):
    '''
    AR-53: Fix dual and 'more than one' nationality individuals.
    '''
    def test_modeling_ar53(self):
        output = self.run_pipeline('ar53')

        import pprint
        people = output['model-person']
        person = people['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,LAEREBEKE%2C%20JOSEPH%20VAN']
        classification = {c['_label'] for c in person.get('classified_as', [])}
        self.assertEqual(classification, {'Flemish', 'Belgian'})

if __name__ == '__main__':
    unittest.main()