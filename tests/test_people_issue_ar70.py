#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestPeoplePipelineOutput, classified_identifiers

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR70(TestPeoplePipelineOutput):
    '''
    AR-70: Add occupation to Person model and then populate it with person roles that tie to aat
    '''
    def test_modeling_ar70(self):
        output = self.run_pipeline('ar70')

        people = output['model-person']
        groups = output['model-groups']

        # these are numbered according the the line number in the source csv file
        person1 = people['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,RUBENS%2C%20PETER%20PAUL']
        person2 = people['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,ALLAN%2C%20DAVID']
        person3 = people['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,CROME%2C%20JOHN']
        person4 = people['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Cartwright%2C%20William']
        person6 = people['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Bryan%2C%20Michael']
        person8 = people['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Winstanley%2C%20Thomas']
        person9 = people['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Coppin%2C%20Daniel']

        group5 = groups['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,London%2C%20England%2C%20UK.%20%20Dulwich%20Picture%20Gallery']
        group7 = groups['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Staten%20Island%2C%20NY%2C%20USA.%20%20Staten%20Island%20Institute%20of%20Arts%20%26%20Sciences']

        self.verify_classification(person1, {'Artist'})
        self.verify_classification(person2, {'Artist', 'Collector'})
        self.verify_classification(person3, {'Artist', 'Collector', 'Dealer'})
        self.verify_classification(person4, {'Collector'})
        self.verify_classification(group5, {'Museum'})
        self.verify_classification(person6, {'Dealer'})
        self.verify_classification(group7, {'Institution'})
        self.verify_classification(person8, {'Collector', 'Dealer'})
        self.verify_classification(person9, {'Artist', 'Dealer'})

    def verify_classification(self, data, expected):
        actual = {c['_label'] for c in data.get('classified_as', [])}
        self.assertTrue(expected.issubset(actual))


if __name__ == '__main__':
    unittest.main()