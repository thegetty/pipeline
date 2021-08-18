#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifiers

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR64(TestSalesPipelineOutput):
    def test_modeling_ar64(self):
        '''
        AR-62: Fix group "Circle of None" record labeling
        AR-64: Fix group "School of None" record labeling
        AR-65: Fix group "PupilGroup of None" record labeling
        AR-66: Fix group "Workshop of None" record labeling
        '''
        output = self.run_pipeline('ar64')
        objects = output['model-object']
        groups = output['model-groups']
        
        obj1 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B5261,0081,1928-04-24']
        obj2 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,F-A1177,0185,1798-02-22']
        obj3 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-2239,0358,1938-11-23']
        obj4 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-2748,0119,1942-06-02']

        self.verifyGroup(groups, obj1, 'School', 'VENNE, ADRIAEN PIETERSZ. VAN DE')
        self.verifyGroup(groups, obj2, 'PupilGroup', 'VALENCIENNES, PIERRE HENRI DE')
        self.verifyGroup(groups, obj3, 'Workshop', 'BASSANO, JACOPO (JACOPO DA PONTE)')
        self.verifyGroup(groups, obj4, 'Circle', 'MEULEN, ADAM FRANS VAN DER')

    def verifyGroup(self, groups, obj, groupType, name):
        prod = obj['produced_by']
        parts = prod['part']
        
        self.assertEqual(len(parts), 1)
        part = parts[0]
        artists = part['carried_out_by']
        self.assertEqual(len(artists), 1)
        artist = artists[0]
        self.assertEqual(artist['_label'], f'{groupType} of Artist “{name}”')
        
        group_id = artist['id']
        group = groups[group_id]
        formation = group['formed_by']
        self.assertIn('influenced_by', formation)
        influence = formation['influenced_by']
        self.assertEqual(len(influence), 1)
        self.assertEqual(influence[0]['_label'], name)

if __name__ == '__main__':
    unittest.main()
