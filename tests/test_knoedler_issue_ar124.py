#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestKnoedlerPipelineOutput, classification_sets, classification_tree

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR124(TestKnoedlerPipelineOutput):
    def test_modeling_ar124(self):
        '''
        AR-124: Check use of concordance on visual work model of concordance sheets for subjects etc. 
        '''
        output = self.run_pipeline('ar124')
        visual_items = output['model-visual-item']
        
        expected_single_class_aat = {
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,8769-VisItem' : {'http://vocab.getty.edu/aat/300139140'},
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,24-VisItem' : {'http://vocab.getty.edu/aat/300386044'},
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,30-VisItem' : {'http://vocab.getty.edu/aat/300015636'},
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,A2214-VisItem' : {'http://vocab.getty.edu/aat/300015637'},
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,A7628-VisItem' : {'http://vocab.getty.edu/aat/300015638'},
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,A1613-VisItem' : {'http://vocab.getty.edu/aat/300139140'},
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,A3676-VisItem' : {'http://vocab.getty.edu/aat/300139140'},
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,7614-VisItem' :  {'http://vocab.getty.edu/aat/300015636'},
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,1481-VisItem' : {'http://vocab.getty.edu/aat/300015636'},
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,A562-VisItem' : {'http://vocab.getty.edu/aat/300386045'},
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,2569-VisItem' : {'http://vocab.getty.edu/aat/300139140'},
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,1132-VisItem' : {'http://vocab.getty.edu/aat/300386045'},
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,3542-VisItem' : {'http://vocab.getty.edu/aat/300015636'},
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,A2360-VisItem' : {'http://vocab.getty.edu/aat/300015636'},
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,A682-VisItem' : {'http://vocab.getty.edu/aat/300015637'},
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,A1269-VisItem' : {'http://vocab.getty.edu/aat/300015636'},
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,Internal,K-19984-VisItem' : {'http://vocab.getty.edu/aat/300139140'},
        }

        expected_multiple_class_aat = {
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,11040-VisItem' : {
                'http://vocab.getty.edu/aat/300235692', 'http://vocab.getty.edu/aat/300117546','http://vocab.getty.edu/aat/300015636'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,15680-VisItem' : {
                'http://vocab.getty.edu/aat/300015424', 'http://vocab.getty.edu/aat/300015636'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,A3370-VisItem' : {
                'http://vocab.getty.edu/aat/300234093', 'http://vocab.getty.edu/aat/300386045'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,483-VisItem' : {
                'http://vocab.getty.edu/aat/300386045', 'http://vocab.getty.edu/aat/300410510', 'http://vocab.getty.edu/aat/300234093'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,51-VisItem' : {
              'http://vocab.getty.edu/aat/300139140', 'http://vocab.getty.edu/aat/300124520'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,10307-VisItem' : {
                'http://vocab.getty.edu/aat/300386045', 'http://vocab.getty.edu/aat/300055985'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,1061-VisItem' : {
                'http://vocab.getty.edu/aat/300386045', 'http://vocab.getty.edu/aat/300410510'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,7029-VisItem' : {
                'http://vocab.getty.edu/aat/300386045', 'http://vocab.getty.edu/aat/300202507'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,13305-VisItem' : {
                'http://vocab.getty.edu/aat/300139140', 'http://vocab.getty.edu/aat/300015424'
            },
        }

        expected_single_repr_aat = {
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,A2214-VisItem' : {'http://vocab.getty.edu/aat/300386296'},
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,A3370-VisItem' : {'http://vocab.getty.edu/aat/300189808'},
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,A3676-VisItem' : {'http://vocab.getty.edu/aat/300025450'},
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,7614-VisItem' :  {'http://vocab.getty.edu/aat/300004792'},
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,1481-VisItem' :  {'http://vocab.getty.edu/aat/300189808'},
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,10307-VisItem' : {'http://vocab.getty.edu/aat/300189808'},
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,A562-VisItem' :  {'http://vocab.getty.edu/aat/300185692'},
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,2569-VisItem' :  {'http://vocab.getty.edu/aat/300189808'},
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,3542-VisItem' :  {'http://vocab.getty.edu/aat/300008057'},
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,A682-VisItem' :  {'http://vocab.getty.edu/aat/300025450'},

        }

        abstract = 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,A1302-VisItem' 

        for id, aat_url in expected_single_class_aat.items():
            self.assertEqual(classification_sets(visual_items[id],key='id'), aat_url)

        for id, aat_urls in expected_multiple_class_aat.items():
            self.assertEqual(classification_sets(visual_items[id], key='id'), aat_urls)
        
        for id, aat_urls in expected_single_repr_aat.items():
            self.assertEqual(classification_sets(visual_items[id], classification_key="represents_instance_of_type", key='id'), aat_urls)
        
        self.assertEqual(classification_tree(visual_items[abstract], key='id'), {
            'http://vocab.getty.edu/aat/300108127' : { 'http://vocab.getty.edu/aat/300015646' : {}}
        })


if __name__ == '__main__':
    unittest.main()
