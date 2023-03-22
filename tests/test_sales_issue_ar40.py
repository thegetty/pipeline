#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifiers

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR40(TestSalesPipelineOutput):
    def test_modeling_ar40_1(self):
        '''
        AR-40: Associate Production to sale row record for attribution modifiers
        '''
        output = self.run_pipeline('ar40')
        objects = output['model-object']
        
        obj1 = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,B-213,0005,1813-02-22']
        prod = obj1['produced_by']
        attrs = prod['attributed_by']

		# assert that attribute assignments on the Production have a reference
		# to the sales record (entry in the sale catalog)
        self.assertEqual(len(attrs), 2)
        for attr in attrs:
            self.assertIn('used_specific_object', attr)
            used_objs = attr['used_specific_object']
            self.assertEqual(len(used_objs), 1)
            used = used_objs[0]
            self.assertIn('Sale recorded in catalog: B-213 0005 (1813-02-22)', used['_label'])

    def test_modeling_ar40_2(self):
        '''
        AR-40: Attribution for Production (attribution modifiers) not associated to row record that supports attribution
        '''
        output = self.run_pipeline('ar40')
        objects = output['model-object']

        expected =  {
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,B-A138,0022,1774-05-30' : {
                'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#ASSIGNMENT,Artist-0,OBJ,B-A138,0022,1774-05-30-Production' : {
                    'used_specific_object' : 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,B-A138,RECORD,110',
                    'verbatim_mod' : 'attributed'
                }
            },
           'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,B-A13,0086,1738-07-21' : {
                'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#ASSIGNMENT,Artist-0,OBJ,B-A13,0086,1738-07-21-Production' : {
                    'used_specific_object' : 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,B-A13,RECORD,6688',
                    'verbatim_mod' : '?'
                }
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-36,0092,1801-05-09' : {
                'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#ASSIGNMENT,Artist-0,OBJ,Br-36,0092,1801-05-09-Production' : {
                    'used_specific_object' : 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,Br-36,RECORD,149539',
                    'verbatim_mod' : 'changed from Thys, P.'
                },
                'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#ASSIGNMENT,Artist-1,OBJ,Br-36,0092,1801-05-09-Production' : {
                    'used_specific_object' : 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,Br-36,RECORD,149539',
                    'verbatim_mod' : 'changed to Boeckhorst'
                }
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,B-A385,0121,1800-06-10' : {
                'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#ASSIGNMENT,NonArtist-0,OBJ,B-A385,0121,1800-06-10-Production' : {
                    'used_specific_object' :'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,B-A385,RECORD,100016',
                    'verbatim_mod' : 'manner'
                }
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,B-213,0005,1813-02-22' : {
                'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#ASSIGNMENT,NonArtist-0,OBJ,B-213,0005,1813-02-22-Production' : {
                    'used_specific_object' :'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,B-213,RECORD,23408',
                    'verbatim_mod' : 'manner; or Vrancx, manner; copy by'
                },
                'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#ASSIGNMENT,NonArtist-1,OBJ,B-213,0005,1813-02-22-Production' : {
                    'used_specific_object' :'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,B-213,RECORD,23408',
                    'verbatim_mod' : 'manner; or Francken, manner; copy by'
                }
            },
        }

        self.verify_results(objects, expected)

    def verify_results(self, objects: dict, expected: dict):
        for groupdid, productions in expected.items():
            self.assertIn(groupdid, objects)
            group = objects[groupdid]
            self.assertIn('produced_by', group)
            produced_by = group['produced_by']
            self.assertIn('attributed_by', produced_by)
            attributed_by = produced_by['attributed_by']
            self.assertEqual(len(attributed_by), len(productions))

            for productionid, data in productions.items():
                production = [p for p in attributed_by if p['id'] == productionid][0]
                self.assertEquals(data['used_specific_object'], production['used_specific_object'][0]['id'])
                self.assertEquals(data['verbatim_mod'], production['referred_to_by'][0]['content'])


if __name__ == '__main__':
    unittest.main()
