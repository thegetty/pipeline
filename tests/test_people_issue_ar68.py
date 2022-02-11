#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestPeoplePipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR68(TestPeoplePipelineOutput):
    '''
    AR-68: Suppress professional activities from groups
    '''
    def test_modeling_ar68(self):
        output = self.run_pipeline('ar68')
        groups = output['model-groups']
        
        # This is a corporate_body Group, and so should not have any professional activities carried out.
        # However, it *should* have a sojourn carried out (and since this is a corporate body, the sojourn is an Establishment activity)
        group1 = groups['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Nebraska%20Art%20Association']
        activities = group1['carried_out']
        self.assertEqual(len(activities), 1)
        cls = activities[0]['classified_as']
        self.assertEqual(len(cls), 1)
        cl = cls[0]
        self.assertEqual(cl['_label'], 'Establishment')
        
        # This is a generic group that should have professional activity modeled based on the century active
        group2 = groups['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#GROUP,AUTH,%5BFRENCH%20-%2015TH%20C.%5D']
        self.assertIn('carried_out', group2)
        activities = group2['carried_out']
        self.assertEqual(len(activities), 1)
        activity = activities[0]
        self.assertEqual(activity['_label'], 'Professional activity of French persons in the 15th century')
        self.assertEqual(classified_identifier_sets(activity['timespan']), {})

        # This is a corporate_body Group that has a century active, so should have a professional activity based on that date data
        group3 = groups['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Macon%2C%20GA%2C%20USA.%20%20Wesleyan%20College']
        self.assertIn('carried_out', group3)
        activities = [a for a in group3['carried_out'] if a['_label'].startswith('Professional activity')]
        self.assertEqual(len(activities), 1)
        activity = activities[0]
        self.assertEqual(activity['_label'], 'Professional activity of Macon, GA, USA.  Wesleyan College')
        # the timespan has a Name which holds the verbatim century_active value
        self.assertEqual(classified_identifier_sets(activity['timespan']), {
        	None: {'19th-20th'}
        })

if __name__ == '__main__':
    unittest.main()