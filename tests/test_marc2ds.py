#-------------------------------------------------------------------------------
# Name:         test_marc2ds
# Purpose:      Unit tests for the marc2ds module
#
# Author:      Jeremy Nelson
#
# Created:     2014/04/25
# Copyright:    (c) Jeremy Nelson, Colorado College 2014
# Licence:     MIT
#-------------------------------------------------------------------------------
import os
import rdflib
import sys
import unittest
from pymongo import MongoClient
sys.path.append("E:/tiger-catalog/")
from catalog.mongo_datastore.ingesters.marc2ds import MARC21toBIBFRAMEIngester



class TestMARC21toBIBFRAMEIngester(unittest.TestCase):

    def setUp(self):
        self.ingester = MARC21toBIBFRAMEIngester(
            mongo_client=MongoClient(port=27018))
        self.pride_prejudice_graph  = rdflib.Graph()
##        self.pride_prejudice_graph.parse(
##            os.path.join(os.path.abspath(os.path.dirname(__file__)),
##                        'pride-and-predjudice.rdf'))

    def test_init(self):
        self.assertEquals(self.ingester.mongo_client.port,
                          27018)
        self.assertFalse(self.ingester.saxon_jar_location)
        self.assertFalse(self.ingester.saxon_xqy_location)
        self.assertEquals(len(self.ingester.graph_ids), 0)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
