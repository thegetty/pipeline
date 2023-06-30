#!/usr/bin/env python3
import unittest

from cromulent import vocab

from tests import TestKnoedlerPipelineOutput


vocab.add_attribute_assignment_check()

class PIRModelingTest_AR266(TestKnoedlerPipelineOutput):
    '''
    AR-266: Knoedler: Update ETL Transform: TGN Place Reconciliation Strategy
    '''
    def test_modeling_ar266(self):
        output = self.run_pipeline('ar266')
        people = output['model-person']
        groups = output['model-groups']
        
        person_except_residence = {
            "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Tooth%2C%20%28Arthur%29%2C%20and%20Sons" : 'London',
            "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Palmer%2C%20Lowell%20M." : '206 Clinton Ave., Brooklyn, NY, USA',
            "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Wallis%20and%20Son" : 'London',
            "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Latham%2C%20Milton%20Slocum" : 'San Francisco',
            "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Catlin%2C%20Daniel" : 'no street address, Saint Louis, MO, USA',
            "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,SOOMBE%2C%20MR." : 'Bylaugh Hall, Norfolk',
            "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Sulley%2C%20%28Arthur%20J.%29%2C%20and%20Co." : "London",
        }

        group_except_residence = {
            "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#ORG,NAME,National%20Gallery%20of%20Victoria,PLACE,Melbourne%2C%20Victoria%2C%20Australia" : "Melbourne"
        }
        
        #self.assertEqual()
        for id, res in person_except_residence.items():
            person_residence = people[id]['residence']
            for pr in person_residence:
                self.assertIn(res, pr['_label'])

        for id, res in group_except_residence.items():
            group_residence = groups[id]['residence']
            for gr in group_residence:
                self.assertIn(res, gr['_label'])

if __name__ == '__main__':
    unittest.main()