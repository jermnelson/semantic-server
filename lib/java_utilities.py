#-------------------------------------------------------------------------------
# Name:        java_utilities.py
# Purpose:     Module uses jython to manipulate various Java classes for use
#              in the Catalog Pull Platform's MongoDB
#
# Author:      Jeremy Nelson
#
# Created:     2014/04/24
# Copyright:   (c) Jeremy Nelson, Colorado College 2014
# Licence:     MIT
#-------------------------------------------------------------------------------
import datetime
import json
import os
import sys
import tempfile

PROJECT_DIR = os.path.split(os.path.realpath(__file__))[0]
JAR_DIR = os.path.join(PROJECT_DIR,
                       "lib")
for jar_file in os.listdir(JAR_DIR):
    if os.path.splitext(jar_file)[-1].endswith('.jar'):
        sys.path.append(os.path.join(JAR_DIR,
                                     jar_file))

import com.mongodb.Mongo as MongoClient
from java.io import ByteArrayOutputStream, File, FileInputStream
from java.io import FileOutputStream
from java.util import Properties
from javax.xml.parsers import DocumentBuilder, DocumentBuilderFactory
from org.w3c.dom import Document, Element, Node, NodeList
from javax.xml.transform.dom import DOMSource, DOMResult
from javax.xml.transform.stream import StreamSource
from net.sf import saxon
from org import marc4j
from org.bson.types import ObjectId
from org.w3c.dom import Document


class MARCtoBIBFRAMEIngester(object):
    """Catalog Pull Platform Class for converting MARC records into BIBFRAME
    entities suitable for ingestion into the Semantic Server's instance of
    MongoDB.

    Run with all defaults

    >> ingester = MARCtoBIBFRAMEIngester()
    >> print(ingester.mongo_client.databaseNames)
    """

    def __init__(self, **kwargs):
        """Creates an MARC to BIBFRAME ingester"""
        self.mongo_client = kwargs.get("mongo_client", MongoClient())
        self.saxon_xqy = kwargs.get('saxon_xqy',
                                    os.path.join('marc2bibframe',
                                                 'xbin',
                                                 'saxon.xqy'))
        self.marc_db = self.mongo_client.getDB('marc')
        self.bibframe = self.mongo_client.getDB('bibframe')

    def __apply_xquery__(self, raw_xml):
        """Internal method takes the raw MARCXML of a record, applies saxon.sqy
        and returns a string of the RDFXML.

        Args:
            raw_xml(str): MARCXML string

        Returns:
            str: String of BIBFRAME RDFXML
        """
        configuration = saxon.Configuration()
        context = configuration.newStaticQueryContext()
        dynamic_context = saxon.query.DynamicQueryContext(configuration)
        props = Properties()
        props.setProperty()
        saxon_xqy_file = FileInputStream(self.saxon_xqy)
        context.setBaseURI(
            File(saxon_xqy).toURI().toString())
        complied_query = context.compileQuery(saxon_xqy_file, None)


    def __as_dict__(self, record):
        """Internal method takes a MARC record converts to JSON that is returned
        as a Python dictionary

        Args:
            record(org.marc4j.Record): MARC record

        Returns:
            dict: Dictionary from MARC record
        """
        json_output = ByteArrayOutputStream()
        json_writer = marc4j.MarcJsonWriter(
            json_output,
            marc4j.MarcJsonWriter.MARC_IN_JSON)
        json_writer.write(record)
        marc_json = self.__mongodbize__(json_output)
        return marc_json

    def __as_xml__(self, record):
        """Internal method transforms MARC record into MARCXML and returns DOM

        Args:
            record(org.marc4j.Record): MARC record

        Returns:
            org.w3c.dom.Document: MARC XML DOM
        """
        xml_stream = ByteArrayOutputStream()
        xml_writer = marc4j.MarcXmlWriter(xml_stream, True)
        xml_writer.setConverter(marc4j.converter.impl.AnselToUnicode())
        xml_writer.setUnicodeNormalization(True)
        xml_writer.write(record)
        xml_writer.close()
        return xml_stream.toString()

    def __convert_subfields__(self, field):
        """Method converts a MARC JSON field into subfields

        Args:
            field (dict): MARC field as a dict

        Returns:
            dict: Dictionary of subfields for ingestion into MongoDB
        """
        new_subfields = OrderedDict()
        if 'subfields' in field:
            for row in field['subfields']:
                subfield = row.keys()[0]
                value = row.get(subfield)
                if subfield in new_subfields:
                    if type(new_subfields[subfield]) == list:
                        new_subfields[subfield].append(value)
                    else:
                        org_subfield = new_subfields[subfield]
                        new_subfields[subfield] = [org_subfield,
                                                   value]
                else:
                    new_subfields[subfield] = value
        field['subfields'] = new_subfields
        return field

    def __mongodbize__(self, raw_json):
        """Method takes raw JSON string, converts to hash-model for MARC JSON
        ingestion into MongoDB.

        Args:
            raw_json(str): Raw text string

        Returns:
            dict: Dictionary for ingestion into MongoDB
        """
        record_dict = json.loads(raw_json)
        for row in record_dict['fields']:
            tag = row.keys()[0]
            contents = self.__convert_subfields__(row.get(tag))
            if tag in new_fields:
                if type(new_fields[tag]) == list:
                    new_fields[tag].append(contents)
                else:
                    org_field = new_fields[tag]
                    new_fields[tag] = [org_field, contents]
            else:
                new_fields[tag] = contents
        record_dict['fields'] = new_fields
        return record_dict

    def __get_or_ingest_marc__(self, record):
        """Internal method either retrieves an existing MARC Mongo ID or
        ingests and returns a new MARC Mongo ID from the Semantic Server.

        Args:
            record(org.marc4j.Record): MARC record

        Returns:
            org.bson.ObjectId: ObjectId of either existing or new MARC entity
        """
        type_of_record = record.getLeader().toString()[6]
        if type_of_record == 'z':
            collection_name = 'authority'
        else:
            collection_name = 'bibliographic'
        marc_collection = self.marc_db.getCollection(collection_name)
        marc_id = None
        # First checks collection for matching System Numbers
        for row in record.getVariableFields('035'):
            system_number = row.getSubfield('a').getData()
            marc_id = marc_collection.findOne(
                {"fields": {"035": {"subfields": {"a": system_number}}}},
                {"_id":1})
            if marc_id is not None:
                return marc_id
        # Insert MARC into the collection if no marc_id
        marc_json = self.__as_dict__(record)
        return marc_collection.insert(marc_json)

    def ingest(self, record):
        """Method takes a MARC record, retrieves or ingests the record as JSON
        into the Semantic Server's Mongo marc database, and then runs LOC's
        xqueries to produce BIBFRAME linked data that is ingested into the Mongo
        BIBFRAME database.

        Args:
            record(org.marc4j.Record): MARC record

        Returns:
            list: List of primary MARC and BIBFRAME Work Mongo IDs
        """
        marc_id = self.__get_or_ingest_marc__(record)

    def batch(self, marc_filename):
        """Method runs a batch ingest of multiple records from a MARC file

        Args:
            marc_filename (str): Complete path and filename of a MARC file
        """
        marc_file = FileInputStream(marc_filename)
        marc_reader = marc4j.MarcStreamReader(marc_file)
        while marc_reader.hasNext():
            try:
                self.ingest(marc_reader.next())
            except:
                print(sys.exc_info())



def marc2bibframe(**kwargs):
    """Function takes a MARC record, extracts XML, transforms to BIBFRAME RDFXML

    Args:
        mongo_client (com.mongodb.Mongo): Mongo Client
        record (org.marc4j.Record): MARC21 record
        saxon_xqy (str): Full path to marc2bibframe/xbin/saxon.xqy
    """





    xml_producer = marc4j.MarcXmlReader()

    xml_writer.close()
    configuration = saxon.Configuration()
    context = configuration.newStaticQueryContext()
    saxon_xqy_file = FileInputStream(saxon_xqy)
    context.setBaseURI(
        File(saxon_xqy).toURI().toString())
    complied_query = context.compileQuery(saxon_xqy_file, None)



def marcfile2bibframe(**kwargs):
    marc_filename = kwargs.get('marc_filename')

    mongo_host = kwargs.get('mongo_host', "localhost")
    mongo_port = kwargs.get('mongo_host', 27017)
    mongo_client = MongoClient(mongo_host, mongo_port)
    saxon_xqy = kwargs.get('saxon_xqy')
    while marc_reader.hasNext():
        try:
            record = marc_reader.next()
            marc2bibframe(mongo_client=mongo_client,
                          record=record,
                          saxon_xqy=saxon_xqy)
        except:
            print(sys.exc_info())






def main():
    pass

if __name__ == '__main__':
    main()
