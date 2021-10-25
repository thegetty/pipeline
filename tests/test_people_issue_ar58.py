#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestPeoplePipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR58(TestPeoplePipelineOutput):
    '''
    AR-58: Improve modeling of location information in PEOPLE
    '''
    def test_modeling_ar58(self):
        output = self.run_pipeline('ar58')

        people = output['model-person']
        groups = output['model-groups']
        
        # A Group should have sojourns modeled as Establishment

        # This is a group with a single sojourn with no date range
        group1 = groups['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Z%C3%BCrich%2C%20Switzerland.%20%20Foundation%20E.G.%20B%C3%BChrle']
        self.verify_group_sojourn(group1, [{'place': 'Zollikerstrasse 172, 8008, Zürich, Switzerland'}])
        
        # People should have sojourns modeled as Residing

        # This is a person with a single sojourn with no date range.
        person1 = people['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,ASHFORD%2C%20WILLIAM']
        self.verify_person_sojourn(person1, {'Sandymount, Dublin, Ireland': {}})

        # This is a person with a single sojourn with no date range.
        person2 = people['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,AGASSE%2C%20JACQUES%20LAURENT']
        self.verify_person_sojourn(person2, {'London, England, UK': {}})

        # This is a person with 3 sojourns, 2 of which have a timespan
        person3 = people['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,ARNALD%2C%20GEORGE']
        self.verify_person_sojourn(person3, {
            'No.5 Fitzroy St, London, England, UK': {
                'begin_of_the_begin': '1815-01-01T00:00:00Z',
                'end_of_the_end': '1816-01-01T00:00:00Z',
            },
            'No.33 Newman St, London, England, UK': {
                'begin_of_the_begin': '1804-01-01T00:00:00Z',
                'end_of_the_end': '1805-01-01T00:00:00Z',
            },
            'London, England, UK': {}
        })
        
        # This is a person with a single sojourn with two associated notes
        person4 = people['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Derby%2C%20Edward%20George%20Villiers%20Stanley%2C%2017th%20Earl%20of']
        self.verify_person_sojourn(person4, {
            'Knowsley Hall, Merseyside, England, UK': {
                'notes': ['from Sales Book 10, 1912-1916, f.165', 'near Liverpool']
            }
        })

    def verify_person_sojourn(self, person, expected_sojourns):
        self.assertIn('carried_out', person)
        activities = [c for c in person['carried_out'] if 'Residing' in {l['_label'] for l in c['classified_as']}]
        self.assertEqual(len(activities), len(expected_sojourns))

		# assert that the Residing type has a metatype of Location
        metatypes = set([c['id'] for a in activities for cl in a.get('classified_as', []) for c in cl.get('classified_as', [])])
        self.assertEqual(metatypes, {'http://vocab.getty.edu/aat/300393211'})

        for a in activities:
            places = a['took_place_at']
            self.assertEqual(len(places), 1)
            place = places[0]
            place_name = place['_label']
            expected = expected_sojourns[place_name]
            
            if 'notes' in expected:
                expected_notes = set(expected['notes'])
                actual_notes = {n['content'] for n in a['referred_to_by']}
                self.assertTrue(expected_notes.issubset(actual_notes))

            for k in ('begin_of_the_begin', 'end_of_the_end'):
                if k in expected:
                    self.assertEqual(expected[k], a['timespan'][k])
            del expected_sojourns[place_name]

    def verify_group_sojourn(self, group, sojourns):
        place_names = set([d['place'] for d in sojourns if 'place' in d])
        self.assertIn('carried_out', group)
        activities = [c for c in group['carried_out'] if 'Establishment' in {l['_label'] for l in c['classified_as']}]
        self.assertEqual(len(activities), len(sojourns))

		# assert that the Establishment type has a metatype of Location
        metatypes = set([c['id'] for a in activities for cl in a.get('classified_as', []) for c in cl.get('classified_as', [])])
        self.assertEqual(metatypes, {'http://vocab.getty.edu/aat/300393211'})
        
        for a in activities:
            places = a['took_place_at']
            self.assertEqual(len(places), 1)
            place = places[0]
            self.assertIn(place['_label'], place_names)
            place_names.remove(place['_label'])


if __name__ == '__main__':
    unittest.main()
