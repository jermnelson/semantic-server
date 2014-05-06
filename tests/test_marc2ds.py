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
import shutil
import sys
import tempfile
import unittest
from bson import ObjectId
from pymongo import MongoClient
sys.path.append("E:/semantic-server/")
from ingesters.marc2ds import MARC21toBIBFRAMEIngester
from ingesters.marc2ds import MARC21toSchemaOrgIngester


MONGO_TEST_PORT = 27018

class TestMARC21toBIBFRAMEIngester(unittest.TestCase):

    def setUp(self):
        self.mongo_temp = MongoClient(port=MONGO_TEST_PORT)
        self.bibframe = self.mongo_temp.conn.bibframe
        self.ingester = MARC21toBIBFRAMEIngester(
            mongo_client=self.mongo_temp)
        self.africa_graph = rdflib.Graph()
        africa_path = os.path.join(
            os.path.abspath(
                os.path.dirname(__file__)),
                    'africa-in-the-world.rdf')
        self.africa_graph.parse(africa_path, format='xml')

    def test_init(self):
        self.assertEquals(self.ingester.mongo_client.port,
                          MONGO_TEST_PORT)
        self.assertFalse(self.ingester.saxon_jar_location)
        self.assertFalse(self.ingester.saxon_xqy_location)
        self.assertEquals(len(self.ingester.graph_ids), 0)

    def test__add_entity__(self):
        work = rdflib.URIRef('http://catalog/ebr10846209')
        work_id = self.ingester.__add_entity__(work,
                                               self.africa_graph,
                                               self.mongo_temp.bibframe.Work)
        self.assert_(work_id)

    def test__convert_subfields__(self):
        field245 = {
            u'ind1': u'1',
            u'ind2': u'0',
            u'subfields': [
                {u'a': u"Report of the Woman's Council of Defense for Colorado :"},
                {u'b': u'from November 30, 1917to November 30, 1918.'}]}
        new245 = self.ingester.__convert_subfields__(field245)
        self.assertEquals(
            new245.get('subfields').get('a'),
            u"Report of the Woman's Council of Defense for Colorado :")
        self.assertEquals(
            new245.get('subfields').get('b'),
            u'from November 30, 1917to November 30, 1918.')

    def test__expand_classification__(self):
        classification = self.africa_graph.value(
            predicate=MARC21toBIBFRAMEIngester.RDF_TYPE_URI,
            object=rdflib.URIRef(u'http://bibframe.org/vocab/Classification'))
        self.assert_(classification)
        output = self.ingester.__expand_classification__(
            classification,
            self.africa_graph)
        self.assertEquals(output['ddc']['label'],
                          '960.32')
        self.assertEquals(output['ddc']['classificationEdition'],
                          'full')
        self.assertEquals(output['ddc']['classificationNumber'],
                          '960.32')


    def test__get_collection__(self):
        work = rdflib.URIRef('http://catalog/ebr10846209')
        self.assertEquals(
            self.ingester.__get_collection__(work, self.africa_graph),
            self.mongo_temp.bibframe.Work)
        instance = rdflib.URIRef('http://catalog/ebr10846209instance23')
        self.assertEquals(
            self.ingester.__get_collection__(instance, self.africa_graph),
            self.mongo_temp.bibframe.Instance)
        annotation = rdflib.URIRef('http://catalog/ebr10846209annotation21')
        self.assertEquals(
            self.ingester.__get_collection__(annotation, self.africa_graph),
            self.mongo_temp.bibframe.Annotation)



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
        self.assertIn(self.ingester.__get_type__(instance,
                                                 self.africa_graph),
                      ['Instance', 'Electronic'])
        annotation = rdflib.URIRef('http://catalog/ebr10846209annotation21')
        self.assertEquals(self.ingester.__get_type__(annotation,
                                                     self.africa_graph),
                          'Annotation')
        person = rdflib.URIRef('http://catalog/ebr10846209person7')
        self.assertEquals(self.ingester.__get_type__(person,
                                                     self.africa_graph),
                          'Person')


    def test__get_or_add_entity__(self):
        work = rdflib.URIRef('http://catalog/ebr10846209')
        work_id = self.ingester.__get_or_add_entity__(work,
                                                      self.africa_graph)
        self.assert_(ObjectId(work_id))
        self.assertEquals(type(work_id), str)
        work2_id = self.ingester.__get_or_add_entity__(work,
                                                       self.africa_graph)
        self.assertEquals(work_id, work2_id)

    def test__process_language__(self):
        output = self.ingester.__process_language__(
            rdflib.URIRef("http://id.loc.gov/vocabulary/languages/eng"))
        self.assertEquals(
            output.get('label'),
            'English')
        self.assertEqual(
            output,
            self.ingester.language_labels.get(
                "http://id.loc.gov/vocabulary/languages/eng"))

    def test__process_titles__(self):
        titles = self.ingester.__process_titles__(self.africa_graph)
        title = rdflib.URIRef('http://catalog/ebr10846209title30')
        self.assertEquals(
            type(titles['http://catalog/ebr10846209title30']),
            ObjectId)
        self.assertEquals(
            titles['http://catalog/ebr10846209title6'],
            titles['http://catalog/ebr10846209title30'])

    def test__mongodbize_graph__(self):
        pass

    def test_batch(self):
        pass

    def test__process_instances__(self):
        instances = self.ingester.__process_instances__(self.africa_graph)
        self.assertEquals(len(instances), 3)


    def tearDown(self):
##        pass
        for db_name in self.mongo_temp.database_names():
            self.mongo_temp.drop_database(db_name)


class TestMARC21toSchemaOrgIngester(unittest.TestCase):

    def setUp(self):
        pass

    def test__get_oclc_owi__(self):
        self.assertEquals(MARC21toSchemaOrgIngester().__get_oclc_owi__(17855037),
                          'owi1953698')


    def tearDown(self):
        pass

if __name__ == '__main__':
    unittest.main()
