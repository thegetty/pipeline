#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestPeoplePipelineOutput, classified_identifier_sets, classification_tree

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR111(TestPeoplePipelineOutput):
    '''
    AR-111: Put city active information from person model into residency branch
    '''
    def test_modeling_ar111(self):
        output = self.run_pipeline('ar111')
        groups = output['model-groups']
        people = output['model-person']
        
        rubens = people['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,RUBENS%2C%20PETER%20PAUL']
        self.verifyActiveCity(rubens, {'Antwerp': '1600-1640'})

        angellis = people['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,ANGELLIS%2C%20PIETER%20VAN']
        self.verifyActiveCity(angellis, {
            'Antwerp': None,
            'Dunkirk': None,
            'London': '1719-1728',
            'Rennes': '1731',
            'Rome': None
        })
        
        mueller = people['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,M%C3%9CLLER-URY%2C%20ADOLF']
        self.verifyActiveCity(mueller, {
        	'Airolo (Tessin)': None,
        	'New York': None
        })

    def verifyActiveCity(self, person, expected):
        self.assertIn('carried_out', person)
        locations = {}
        for a in person.get('carried_out', []):
            cl = classification_tree(a)
            if 'Residing' in cl:
                place = a.get('took_place_at', [{}])[0].get('_label')
                date = a.get('timespan', {}).get('identified_by', [{}])[0].get('content')
                locations[place] = date
        self.assertEqual(locations, expected)


if __name__ == '__main__':
    unittest.main()
