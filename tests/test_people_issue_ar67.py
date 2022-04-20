#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestPeoplePipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR67(TestPeoplePipelineOutput):
    '''
    AR-67: Internal vs External Tag for kinds of notes
    '''
    def test_modeling_ar67(self):
        output = self.run_pipeline('ar67')

        people = output['model-person']
        self.verifyPrivateField(people['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,AGASSE%2C%20JACQUES%20LAURENT'])
        self.verifyNoteFields(people['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,SCHOUMAN%2C%20MARTINUS'])

    def verifyNoteFields(self, person):
        # expect there to be four note fields coming from the input fields: text, notes, brief_notes, working_notes
        # expect that the brief_notes field is marked public, while the other three are marked private
        # expect that the working_notes field is classified as a research statement, while the other three are biography statements
        expected = {
            'Bibliography Statement': {'Thieme-Becker;'},
            'Biography Statement': {
                'Martinus Schouman was director of the Dordrecht drawing society Pictura (1800-15)',
                'painter and auctioneer in Dordrecht',
                'was in Breda after 1839'
            },
            'Research Statement': {'There was also an A. Schouman in Dordrecht.'},
            'Source Statement': {'Dutch Sales'},
            'private (general concept)': {
                'Martinus Schouman was director of the Dordrecht drawing society Pictura (1800-15)',
                'There was also an A. Schouman in Dordrecht.',
                'was in Breda after 1839'
            },
            'public (general concept)': {'painter and auctioneer in Dordrecht'}
        }
        self.assertEqual(classified_identifier_sets(person, 'referred_to_by'), expected)

    def verifyPrivateField(self, person):
        self.assertIn('referred_to_by', person)
        refs = [r for r in person['referred_to_by'] if 'was born in Geneva' in r.get('content', '')]
        self.assertEqual(len(refs), 1)
        ref = refs[0]

        # expect the PEOPLE 'text' field to be marked as both private and a Biography Statement
        expected = "was born in Geneva, briefly active in Paris, and resident in London from 1800 onwards.  He put up two of his own paintings for sale in 1801 but both were bought in.  He lived in Goldsmith Street, according to Christie's files, and his collection was sold by Christie's in 1850 after his death.  He was primarily a painter of animals and landscape."
        self.assertEqual(classified_identifier_sets(person, 'referred_to_by'), {
            'Bibliography Statement': {'TB; Thieme-Becker'},
            'Source Statement': {'Sales Index 1751-1800; Sales Index 1-5'},
            'Biography Statement': {expected},
            'private (general concept)': {expected}
        })


if __name__ == '__main__':
    unittest.main()
