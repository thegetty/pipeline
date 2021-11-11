#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestPeoplePipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR57(TestPeoplePipelineOutput):
    PREFIX = 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,'

    '''
    AR-57: Improve modeling of PEOPLE dates
    '''
    def test_modeling_ar57(self):
        output = self.run_pipeline('ar57')

        people = output['model-person']

        # The keys in the following dictionary are binary sets indicating
        # whether the records have the following fields:
        # 
        # 1. period_active
        # 2. century_active
        # 3. birth_date
        # 4. death_date
        #
        # For example, the keys 00101 indicates the record has values for period_active and active_city_date
        #
        #
        # The expected value is a tuple with three values:
        # 
        # 1. The date string for the BEGINNING of the expected professional activity (or None)
        # 2. The date string for the END of the expected professional activity (or None)
        # 3. The verbatim string used to construct the previous two values.
        #    NOTE that this verbatim string may not give exact bounds, as the person's
        #    birth and death dates may have been used to clamp the timespan narrower than
        #    the verbatim string would indicate.

        people_data = {
            '1000': {
                # only period_active=1910
                'key': f'{self.PREFIX}AUGLAY%2C%20AUGUSTE',
                'expected': {('1910-01-01T00:00:00Z', '1911-01-01T00:00:00Z', '1910')},
            },
            '1001': {
                # period_active=1765 with an irrelevant death_date
                'key': f'{self.PREFIX}Lebrun%2C%20Pierre',
                'expected': {('1765-01-01T00:00:00Z', '1766-01-01T00:00:00Z', '1765')},
            },
            '1010': {
                # period_active=1858-1890 with an irrelevant birth_date
                'key': f'{self.PREFIX}Escribe%2C%20Eug%C3%A8ne%20Jean%20Louis',
                'expected': {('1858-01-01T00:00:00Z', '1891-01-01T00:00:00Z', '1858-1890')},
            },
            '1011': {
                # period_active=18th-19th with birth_date=1743, death_date=1820
                'key': f'{self.PREFIX}Chandelle%2C%20Andreas%20Joseph',
                'expected': {('1743-01-01T00:00:00Z', '1821-01-01T00:00:00Z', '18th-19th')},
            },
            '1100': {
                # period_active=1850 with irrelevant century_active=19th
                'key': f'{self.PREFIX}CIULI%2C%20GIULIO%20%5BUNIDENTIFIED%5D',
                'expected': {('1850-01-01T00:00:00Z', '1851-01-01T00:00:00Z', '1850')},
            },
            '1101': {
                # period_active=bef.1888, century_active=19th-20th, death_date=1938
                'key': f'{self.PREFIX}D%C3%96RING%2C%20ADOLF%20GUSTAV',
                'expected': {('1883-01-01T00:00:00Z', '1889-01-01T00:00:00Z', 'bef.1888')},
            },
            '1110': {
                # period_active=1876, century_active=19th, death_date=1849
                'key': f'{self.PREFIX}LIECK%2C%20JOSEPH',
                'expected': {('1876-01-01T00:00:00Z', '1877-01-01T00:00:00Z', '1876')},
            },
            '1111': {
                # period_active=1945, century_active=20th, birth_date=1907, death_date=1967
                'key': f'{self.PREFIX}Bilbo%2C%20Jack',
                'expected': {('1945-01-01T00:00:00Z', '1946-01-01T00:00:00Z', '1945')},
            },
            '0000': {
                # no date data
                'key': f'{self.PREFIX}GEYER',
                'expected': set(),
            },
            '0001': {
                # death_date=1922
                'key': f'{self.PREFIX}Lehmann%2C%20Albert',
                'expected': set(),
            },
            '0010': {
                # birth_date=1876
                'key': f'{self.PREFIX}GOULET%2C%20GEORGES',
                'expected': set(),
            },
            '0011': {
                # birth_date=1793, death_date=1873
                'key': f'{self.PREFIX}Parris%2C%20Edmund%20Thomas',
                'expected': set(),
            },
            '0100': {
                # century_active=17th
                'key': f'{self.PREFIX}BRAIN%20%5BUNIDENTIFIED%5D',
                'expected': {('1600-01-01T00:00:00Z', '1700-01-01T00:00:00Z', '17th')},
            },
            '0101': {
                # century_active=18th, death_date=1731
                'key': f'{self.PREFIX}NIGHTINGALE%2C%20ELIZABETH',
                'expected': {('1700-01-01T00:00:00Z', '1732-01-01T00:00:00Z', '18th')},
            },
            '0110': {
                # century_active=19th, birth_date=1844
                'key': f'{self.PREFIX}FROMENTIN%2C%20JULES',
                'expected': {('1844-01-01T00:00:00Z', '1900-01-01T00:00:00Z', '19th')},
            },
            '0111': {
                # century_active=19th, birth_date=1838, death_date=1898
                'key': f'{self.PREFIX}PAULSEN%2C%20FRITZ',
                'expected': {('1838-01-01T00:00:00Z', '1899-01-01T00:00:00Z', '19th')},
            },
        }

        self.assert_results(people, people_data)

    def assert_results(self, model, expected_data):
        total = 0
        assertions = 0
        for p in expected_data.values():
            total += 1
            k = p['key']
            expected = p.get('expected', set())

            if 'expected' in p:
                person = model[k]
                activities = [a for a in person.get('carried_out', []) if a['_label'].startswith('Professional activity of')]
                triples = set()
                for a in activities:
                    ts = a.get('timespan')
                    if ts:
                        begin = ts.get('begin_of_the_begin')
                        end = ts.get('end_of_the_end')
                        label = ts.get('identified_by', [{}])[0].get('content')
                        triples.add((begin, end, label))
                assertions += 1
                self.assertEqual(triples, expected)

        # make sure we tested all expected cases
        self.assertEqual(assertions, len(expected_data))

if __name__ == '__main__':
    unittest.main()
