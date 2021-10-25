#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestPeoplePipelineOutput, classified_identifiers

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR61(TestPeoplePipelineOutput):
    '''
    AR-61: Remove Active classification of professional activities
    '''
    def test_modeling_ar61(self):
        output = self.run_pipeline('ar61')

        people = output['model-person']
        person = people['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,PONFIL%20%5BUNIDENTIFIED%5D']
        activities = person['carried_out']
        self.assertEqual(len(activities), 1)
        activity = activities[0]
        cls = activity['classified_as']
        self.assertEqual(len(cls), 1)
        self.assertEqual(cls[0]['_label'], 'Creating Artwork')

if __name__ == '__main__':
    unittest.main()