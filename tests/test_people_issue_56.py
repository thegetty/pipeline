#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestPeoplePipelineOutput, classified_identifiers

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR56(TestPeoplePipelineOutput):
    '''
    AR-56: Birth Date / Death Date Dates not presented in original form
    '''
    def test_modeling_ar56(self):
        output = self.run_pipeline('ar56')
        uri_prefix = 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,'

        people = output['model-person']
        groups = output['model-groups']

        self.verifyTimeSpanName(people[f'{uri_prefix}WIJNANTS%2C%20JAN'], 'born', '1630/35')

        self.verifyTimeSpanName(people[f'{uri_prefix}AGASSE%2C%20JACQUES%20LAURENT'], 'born', '1767')
        self.verifyTimeSpanName(people[f'{uri_prefix}AGASSE%2C%20JACQUES%20LAURENT'], 'died', '1849')

        self.verifyTimeSpanName(people[f'{uri_prefix}BAERS%2C%20JOHANNES'], 'died', 'aft. 1641')

        self.verifyTimeSpanName(people[f'{uri_prefix}EHRENBERG%2C%20PETER%20SCHUBERT%20VON'], 'born', '1668')

        self.verifyTimeSpanName(people[f'{uri_prefix}GRYEF%2C%20ADRIAEN%20DE'], 'born', 'ca. 1670')
        self.verifyTimeSpanName(people[f'{uri_prefix}GRYEF%2C%20ADRIAEN%20DE'], 'died', '1715')

        self.verifyTimeSpanName(people[f'{uri_prefix}NOORT%2C%20JOAN%20VAN'], 'born', 'ca. 1620')

        self.verifyTimeSpanName(people[f'{uri_prefix}AGASIAS%20OF%20EPHESOS'], 'died', 'ca. 100 BC')

        self.verifyTimeSpanName(people[f'{uri_prefix}ASHLEY%2C%20MARGARET%20JANE'], 'born', '1837/03/07')
        self.verifyTimeSpanName(people[f'{uri_prefix}ASHLEY%2C%20MARGARET%20JANE'], 'died', '1913/02/27')

        self.verifyTimeSpanName(people[f'{uri_prefix}BROOKE%2C%20ROBERT%20DIGBY'], 'born', 'aft. 1704')

        self.verifyTimeSpanName(people[f'{uri_prefix}GIOTTO%20DI%20BONDONE'], 'born', '1276')
        self.verifyTimeSpanName(people[f'{uri_prefix}GIOTTO%20DI%20BONDONE'], 'died', '1337')

        self.verifyTimeSpanName(people[f'{uri_prefix}SALMON%2C%20LOUIS%20ADOLPHE'], 'born', '1806-11')
        self.verifyTimeSpanName(people[f'{uri_prefix}SALMON%2C%20LOUIS%20ADOLPHE'], 'died', '1895')

        self.verifyTimeSpanName(people[f'{uri_prefix}VITRUVIUS%20%28MARCUS%20VITRUVIUS%20POLLIO%29'], 'born', 'ca. 90 BC')
        self.verifyTimeSpanName(people[f'{uri_prefix}VITRUVIUS%20%28MARCUS%20VITRUVIUS%20POLLIO%29'], 'died', 'ca. 20 BC')

        self.verifyTimeSpanName(people[f'{uri_prefix}WEYDEN%2C%20ROGIER%20VAN%20DER'], 'born', '1399/1400')
        self.verifyTimeSpanName(people[f'{uri_prefix}WEYDEN%2C%20ROGIER%20VAN%20DER'], 'died', '1464')

        self.verifyTimeSpanName(people[f'{uri_prefix}WINTER%2C%20ABRAHAM%20HENDRIK'], 'born', 'bef. 1800')
        self.verifyTimeSpanName(people[f'{uri_prefix}WINTER%2C%20ABRAHAM%20HENDRIK'], 'died', '1861')

        self.verifyTimeSpanName(groups[f'{uri_prefix}London%20Arts%20Detroit%20Gallery'], 'formed_by', 'ca. 1950')


    def verifyTimeSpanName(self, person, event, name):
        birth = person[event]
        ts = birth['timespan']
        self.assertIn('identified_by', ts)
        ids = ts['identified_by']
        self.assertEqual(len(ids), 1)
        self.assertEqual(ids[0]['content'], name)

if __name__ == '__main__':
    unittest.main()
