#!/usr/bin/env python3 -B
import unittest

from tests import TestKnoedlerPipelineOutput, classification_sets, classification_tree, classified_identifier_sets
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR144(TestKnoedlerPipelineOutput):
    def test_modeling_ar144(self):
        '''
        AR-144: Model buyer/seller agents in sub-activities
        '''
        output = self.run_pipeline('ar144')
        activities = output['model-activity']
        import json
        
        # Sold to Knoedler by Clark (via Goupil)
        tx1 = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,In,1,192,6']
        self.verifyAgents(tx1, sellers={'Clark, Jonas G.'}, seller_agents={'Goupil et Cie.'}, buyer_agents=set(), buyers={'M. Knoedler & Co.'})
        
        # Sold to Knoedler by Gill (via KILTON)
        tx2 = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,In,6,223,30']
        self.verifyAgents(tx2, sellers={'Gill, James Dwyer'}, seller_agents={'KILTON, W.S.'}, buyer_agents=set(), buyers={'M. Knoedler & Co.'})
        
		# Sold by Knoedler to Howard
        tx3 = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,Out,3,203,11']
        self.verifyAgents(tx3, sellers={'M. Knoedler & Co.'}, seller_agents=set(), buyer_agents=set(), buyers={'Howard, George'})

        # Sold to Knoedler by Tufts (via MATTHEWS)
        tx4 = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,In,3,203,11']
        self.verifyAgents(tx4, sellers={'Tufts, Edwin'}, seller_agents={'MATTHEWS, N.'}, buyer_agents=set(), buyers={'M. Knoedler & Co.'})

    def verifyAgents(self, tx, sellers, seller_agents, buyer_agents, buyers):
        acqs = [p for p in tx['part'] if p.get('type') == 'Acquisition']
        self.assertEqual(len(acqs), 1)
        acq = acqs[0]
        reciever = {p.get('_label') for p in acq['transferred_title_from']}
        sender = {p.get('_label') for p in acq['transferred_title_to']}
        self.assertEqual(sender, buyers)
        self.assertEqual(reciever, sellers)
        seller_agent_parts = [p for p in acq.get('part', []) if "Seller's Agent" in classification_sets(p)]
        buyer_agent_parts = [p for p in acq.get('part', []) if "Buyer's Agent" in classification_sets(p)]

        actual_seller_agents = set([p['_label'] for part in seller_agent_parts for p in part.get('carried_out_by', [])])
        actual_buyer_agents = set([p['_label'] for part in buyer_agent_parts for p in part.get('carried_out_by', [])])
        self.assertEqual(actual_seller_agents, seller_agents)
        self.assertEqual(actual_buyer_agents, buyer_agents)


if __name__ == '__main__':
    unittest.main()
