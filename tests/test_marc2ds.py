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
        self.africa_graph = rdflib.Graph()
        africa_path = os.path.join(
            os.path.abspath(
                os.path.dirname(__file__)),
                    'africa-in-the-world.rdf')
        self.africa_graph.parse(africa_path, format='xml')

    def test_init(self):
        self.assertEquals(self.ingester.mongo_client.port,
                          27018)
        self.assertFalse(self.ingester.saxon_jar_location)
        self.assertFalse(self.ingester.saxon_xqy_location)
        self.assertEquals(len(self.ingester.graph_ids), 0)

    def test__add_entity__(self):
        pass
##        work = rdflib.URIRef('http://catalog/ebr10846209')
##        work_id = self.ingester.__add_entity__(work, self.pride_prejudice_graph)
##        self.assert_(work_id)

    def test__get_type__(self):
        work = rdflib.URIRef('http://catalog/ebr10846209')
        self.assertIn(self.ingester.__get_type__(work, self.africa_graph),
                      [u'Text', u'Work'])
        work = rdflib.URIRef('http://catalog/ebr10846209work19')
        self.assertEquals(self.ingester.__get_type__(work, self.africa_graph),
                          'Work')
        instance = rdflib.URIRef('http://catalog/ebr10846209instance22')
        self.assertIn(self.ingester.__get_type__(instance, self.africa_graph),
                      [u'Instance', u'Monograph', u'Electronic'])
        instance = rdflib.URIRef('http://catalog/ebr10846209instance23')
        self.assertEquals(self.ingester.__get_type__(instance,
                                                     self.africa_graph),
                          'Instance')
        annotation = rdflib.URIRef('http://catalog/ebr10846209annotation21')
        self.assertEquals(self.ingester.__get_type__(annotation,
                                                     self.africa_graph),
                          'Annotation')
        person = rdflib.URIRef('http://catalog/ebr10846209person7')
        self.assertEquals(self.ingester.__get_type__(person,
                                                     self.africa_graph),
                          'Person')




    def test__get_or_add_entity__(self):
        pass

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
