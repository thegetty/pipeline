#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifiers

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR43(TestSalesPipelineOutput):
    '''
    AR-43: Fix 'Attributed to' Modifier use
    '''
    def test_modeling_ar43(self):
        output = self.run_pipeline('ar43')
        objects = output['model-object']
        groups = output['model-groups']
        people = output['model-person']
        
        # "attributed to"
        obj1 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,B-A138,0022,1774-05-30']
        prod1 = obj1['produced_by']
        self.assertIn('attributed_by', prod1)
        attr1 = prod1['attributed_by']
        self.assertEqual({c['_label'] for c in attr1[0]['classified_as']}, {'Possibly'})
        self.assertEqual(len(attr1), 1)
        self.assertEqual(attr1[0]['_label'], 'Possibly attributed to SAVERY (XAVERY)')
        self.assertEqual(attr1[0]['assigned'][0]['id'], 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,SAVERY%20%28XAVERY%29')

        # There are no sub-parts of the production, since all the known
        # information has the 'attributed to' modifier, causing it to be
        # asserted indirectly via an attribution assignment.
        self.assertNotIn('part', prod1)
        
        # "possibly by"
        obj2 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,B-A13,0086,1738-07-21']
        prod2 = obj2['produced_by']
        self.assertIn('attributed_by', prod2)
        attr2 = prod2['attributed_by']
        self.assertEqual(len(attr2), 1)
        self.assertEqual({c['_label'] for c in attr2[0]['classified_as']}, {'Possibly'})
        self.assertEqual(attr2[0]['_label'], 'Possibly by CORNEILLE, JEAN BAPTISTE')
        self.assertEqual(attr2[0]['assigned'][0]['id'], 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,CORNEILLE%2C%20JEAN%20BAPTISTE')
        self.assertNotIn('part', prod2)

        # "copy by", "copy after"
        obj3 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-30,0118,1801-04-27']
        prod3 = obj3['produced_by']
        # copy by
        self.assertIn('part', prod3)
        parts = prod3['part']
        self.assertEqual(len(parts), 1)
        part = parts[0]
        self.assertEqual(part['carried_out_by'][0]['id'], 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,HEAD%2C%20GUY')
        # copy after
        influences = prod3['influenced_by']
        self.assertEqual(len(influences), 1)
        influence_id = influences[0]['id']
        self.assertIn(influence_id, objects)
        influence = objects[influence_id]
        influence_prod = influence['produced_by']
        self.assertIn('part', influence_prod)
        influence_parts = influence_prod['part']
        self.assertEqual(len(influence_parts), 1)
        influence_part = influence_parts[0]
        self.assertEqual(influence_part['carried_out_by'][0]['id'], 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,WILSON%2C%20RICHARD')

        # "attributed to"
        obj4 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-36,0092,1801-05-09']
        prod4 = obj4['produced_by']
        self.assertIn('attributed_by', prod4)
        attr4 = prod4['attributed_by']
        self.assertEqual(len(attr4), 2)
        # formerly attributed to
        attr4_old = [a for a in attr4 if 'BOECKHORST' in a['_label']][0]
        self.assertEqual({c['_label'] for c in attr4_old['classified_as']}, {'Possibly'})
        self.assertEqual(attr4_old['_label'], 'Possibly attributed to BOECKHORST, JOHANN')
        self.assertEqual(attr4_old['assigned'][0]['id'], 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,BOECKHORST%2C%20JOHANN')
        
        # "formerly attributed to", attributed to
        attr4_new = [a for a in attr4 if 'THYS' in a['_label']][0]
        self.assertEqual({c['_label'] for c in attr4_new['classified_as']}, {'Obsolete'})
        self.assertEqual(attr4_new['_label'], 'Formerly attributed to THYS, PIETER')
        self.assertEqual(attr4_new['assigned'][0]['id'], 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,THYS%2C%20PIETER')

        # "school of; copy by", "copy after"
        obj5 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,F-75,0046,1804-05-23']
        prod5 = obj5['produced_by']
        # copy by
        self.assertIn('part', prod5)
        parts = prod5['part']
        self.assertEqual(len(parts), 1)
        part = parts[0]
        self.assertEqual(part['carried_out_by'][0]['id'], 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,LUINI%2C%20BERNARDINO-School')
        # copy after
        influences = prod5['influenced_by']
        self.assertEqual(len(influences), 1)
        influence_id = influences[0]['id']
        self.assertIn(influence_id, objects)
        influence = objects[influence_id]
        influence_prod = influence['produced_by']
        self.assertIn('part', influence_prod)
        influence_parts = influence_prod['part']
        self.assertEqual(len(influence_parts), 1)
        influence_part = influence_parts[0]
        self.assertEqual(influence_part['carried_out_by'][0]['id'], 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,LEONARDO%20DA%20VINCI')

        # "circle of; or", "or"
        obj6 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-1967,0115,1937-06-03']
        import pprint
        prod6 = obj6['produced_by']
        artists = prod6['carried_out_by']
        self.assertEqual(len(artists), 1)
        group = artists[0]
        self.assertEqual(group['type'], 'Group')
        self.assertIn(group['id'], groups)
        group_id = group['id']
        group = groups[group_id]
        self.assertEqual(group['_label'], 'Group containing the artist of Gemälde: “ Schütz-Kreis oder Vetter. 2 Gegenstücke, ähnlich den vorhergehenden. Holz. 24 x 31 cm. (561)”')
        circle = groups['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,SCH%C3%9CZ%2C%20CHRISTIAN%20GEORG%20%281718%29-Circle']
        person = people['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,SCH%C3%9CZ%2C%20CHRISTIAN%20GEORG%20%281758%29']
        for p in (person, circle):
        	membership = {g['id'] for g in p['member_of']}
        	self.assertIn(group_id, membership)

if __name__ == '__main__':
    unittest.main()