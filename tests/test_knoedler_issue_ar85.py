#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestKnoedlerPipelineOutput, classified_identifiers

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR85(TestKnoedlerPipelineOutput):
    '''
    AR-85: Knoedler record enrichment
    '''
    def test_modeling_ar85(self):
        output = self.run_pipeline('ar85')

        objects = output['model-object']
        texts = output['model-lo']

        book = texts['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Text,Book,4']
        page = texts['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Text,Book,4,Page,29']
        row = texts['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Text,Book,4,Page,29,Row,14']

        self.verifyKnoedlerEnrichment_creationPlace(book)
        self.verifyKnoedlerEnrichment_creationPlace(page)

        self.verifyKnoedlerEnrichment_creationTimespan(row, '1884-09-23')

    def verifyKnoedlerEnrichment_creationPlace(self, obj):
        self.assertIn('created_by', obj)
        creation = obj['created_by']

        self.assertIn('took_place_at', creation)
        places = creation['took_place_at']
        self.assertEqual(len(places), 1)
        place = places[0]
        self.assertEqual(place['_label'], 'New York, NY')

        self.verifyKnoedlerCreation(obj)

    def verifyKnoedlerEnrichment_creationTimespan(self, obj, date):
        self.assertIn('created_by', obj)
        creation = obj['created_by']

        self.assertIn('timespan', creation)
        ts = creation['timespan']
        self.assertEqual(ts['_label'], date)

        self.verifyKnoedlerCreation(obj)

    def verifyKnoedlerCreation(self, obj):
        self.assertIn('created_by', obj)
        creation = obj['created_by']
        self.assertIn('carried_out_by', creation)
        creators = creation['carried_out_by']
        self.assertEqual(len(creators), 1)
        creator = creators[0]
        self.assertEqual(creator['_label'], 'M. Knoedler & Co.')


if __name__ == '__main__':
    unittest.main()
