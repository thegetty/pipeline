#!/usr/bin/env python3 -B
import unittest

from tests import TestKnoedlerPipelineOutput, classification_sets
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR103(TestKnoedlerPipelineOutput):
    def test_modeling_ar103(self):
        '''
        AR-103: Align Attribution modifier code in Knoedler with Sales
        '''
        output = self.run_pipeline('ar103')
        objects = output['model-object']

        import pprint, json
        
        # "attributed to"
        hmo1 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,7843']
        self.verifyAttributedToObject(hmo1, 'JACQUE, CHARLES EMILE')

        # "attributed to; school of"
        hmo2 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,13006']
        self.verifyAttributedToObject(hmo2, 'School of CLOUET, FRANÇOIS')

        # "copy after"
        hmo3 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,Internal,K-23732']
        self.verifyCopyAfterObject(hmo3, 'ANGELICO, FRA', objects)

        # "copy by", "copy after"
        hmo4 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,16258']
        self.verifyCopyAfterObject(hmo4, 'GRECO, EL (DOMENICO THEOTOCOPULI)', objects)
        self.verifyArtist(hmo4, 'SARGENT, JOHN SINGER')

        # "follower of"
        hmo5 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,A2022']
        self.verifyArtist(hmo5, 'Follower(s) of CANALETTO (GIOVANNI ANTONIO CANAL)')

        # "possibly by"
        hmo6 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,Internal,K-16119']
        self.verifyAttributedToObject(hmo6, 'RICO ORTEGA, MARTÍN', {'Possibly'})

        # "school of"
        hmo7 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,13462']
        self.verifyArtist(hmo7, 'School of BRIOSCO, ANDREA')

        # "style of; manner of"
        hmo8 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,13556']
        self.verifyObjectInfluence(hmo8, 'LAWRENCE, THOMAS')

    def verifyAttributedToObject(self, hmo, name, classifications=None):
        prod = hmo['produced_by']
        self.assertIn('attributed_by', prod)
        attrs = [a for a in prod['attributed_by'] if a.get('assigned_property') == 'carried_out_by']
        self.assertEqual(len(attrs), 1)
        attr = attrs[0]
        
        # the next two assertions capture the case of 'Possibly by XXX' and 'Possibly attributed to XXX'
        self.assertTrue(attr['_label'].startswith(f'Possibly'))
        self.assertTrue(attr['_label'].endswith(name))
        self.assertIn('assigned', attr)
        people = attr['assigned']
        self.assertEqual(len(people), 1)
        person = people[0]
        self.assertEqual(person['_label'], name)
        
        if classifications:
            self.assertEqual(classification_sets(attr), classifications)

    def verifyArtist(self, hmo, name):
        prod = hmo['produced_by']
        parts = [p for p in prod['part'] if name in p['_label']]
        self.assertEqual(len(parts), 1)
        part = parts[0]
        self.assertIn('carried_out_by', part)
        people = part['carried_out_by']
        self.assertEqual(len(people), 1)
        person = people[0]
        self.assertEqual(person['_label'], name)

    def verifyCopyAfterObject(self, hmo, name, objects):
        prod = hmo['produced_by']
        self.assertIn('influenced_by', prod)
        influences = prod['influenced_by']
        self.assertEqual(len(influences), 1)
        influence_uri = influences[0]['id']
        self.assertIn(influence_uri, objects)
        influence = objects[influence_uri]
        self.verifyArtist(influence, name)

    def verifyObjectInfluence(self, hmo, name):
        prod = hmo['produced_by']
        self.assertIn('attributed_by', prod)
        attrs = [a for a in prod['attributed_by'] if a.get('assigned_property') == 'influenced_by']
        self.assertEqual(len(attrs), 1)
        attr = attrs[0]
        
        self.assertEqual(attr['_label'], f'In the style of {name}')
        self.assertIn('assigned', attr)
        people = attr['assigned']
        self.assertEqual(len(people), 1)
        person = people[0]
        self.assertEqual(person['_label'], name)


if __name__ == '__main__':
    unittest.main()
