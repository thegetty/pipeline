#!/usr/bin/env python3 -B
import unittest

from tests import TestSalesPipelineOutput, classification_sets, classification_tree, classified_identifier_sets
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR144(TestSalesPipelineOutput):
    def test_modeling_ar144(self):
        '''
        AR-144: Model buyer/seller agents in sub-activities
        '''
        output = self.run_pipeline('ar144')
        activities = output['model-activity']
        
        # Sold by Outryve. Bought by Ducq (via agent Heerebons)
        tx1 = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PROV,B-257,1816-07-16,0037']
        self.verifyAgents(tx1, sellers={'Outryve, Jean-Jacques van'}, seller_agents=set(), buyer_agents={'Heerebons'}, buyers={'Ducq (Brugge)'})
        
        # Sold by Earl of Bristol (via agent Cleasby). Bought by Budge.
        tx2 = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PROV,Br-96,1802-04-10,0019']
        self.verifyAgents(tx2, sellers={'Bristol, Frederick Augustus Hervey, 4th Earl of'}, seller_agents={'Cleasby'}, buyer_agents=set(), buyers={'Budge'})

		# Sold by Gruyter (via agent Vinkeles). Bought by G. (via agent Yver).
        tx3 = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PROV,N-103,1806-08-13,0346']
        self.verifyAgents(tx3, sellers={'Gruyter, Willem, Senior'}, seller_agents={'Vinkeles, Jacobus'}, buyer_agents={'Yver, Jan'}, buyers={'G. (Amsterdam)'})

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
