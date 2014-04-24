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
import json
import os
import sys

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
from javax.xml.parsers import DocumentBuilder
from org.w3c.dom import Document, Element, Node, NodeList
from javax.xml.transform.dom import DOMSoure, DOMResult
from javax.xml.transform.stream import StreamSource
from net.sf import saxon
from org import marc4j


def marc2bibframe(**kwargs):
    """Function takes a MARC record, extracts XML, transforms to BIBFRAME RDFXML

    Args:
        mongo_client (com.mongodb.Mongo): Mongo Client
        record (org.marc4j.Record): MARC21 record
        saxon_xqy (str): Full path to marc2bibframe/xbin/saxon.xqy
    """
    mongo_client = kwargs.get("mongo_client",
                              MongoClient())
    record = kwargs.get('record')
    saxon_xqy = kwargs.get('saxon_xqy')
    json_output = ByteArrayOutputStream()
    json_writer = marc4j.MarcJsonWriter(
        json_output,
        marc4j.MarcJsonWriter.MARC_IN_JSON)
    marc_db = mongo_client.getDB('marc')
    bibframe = mongo_client.getDB('bibframe')
    type_of_record = record.getLeader().toString()[6]
    if type_of_record == 'z':
        kind_of_record = record.getVariableField('008').toString()[9]
        if kind_of_record == 'a':
            collection_name = 'name_authority'
    else:
        collection_name = 'bibliographic'
    marc_collection = marc_db.getCollection(collection_name)
    marc_id = None
    # First checks collection for matching System Numbers
    for row in record.getVariableFields('035'):
        system_number = row.getSubfield('a').getData()
        marc_id = marc_collection.findOne(
            {"fields": {"035": {"subfields": {"a": system_number}}}},
            {"_id":1})
        if marc_id is not None:
            break
    # Insert MARC into the collection if no marc_id
    if marc_id is None:
        json_writer.write(record)
        marc_json = mongodbizeMARC(json_output)
        marc_id = marc_collection.insert(marc_json)
    xml_stream = ByteArrayOutputStream()
    xml_writer = marc4j.MarcXmlWriter(xml_stream, True)
    xml_writer.write(record)
    xml_writer.close()
    configuration = saxon.Configuration()
    context = configuration.newStaticQueryContext()
    saxon_xqy_file = FileInputStream(saxon_xqy)
    context.setBaseURI(
        File(saxon_xqy).toURI().toString())
    complied_query = context.compileQuery(saxon_xqy_file, None)



def marcfile2bibframe(**kwargs):
    marc_filename = kwargs.get('marc_filename')
    marc_file = FileInputStream(marc_filename)
    marc_reader = marc4j.MarcStreamReader(marc_file)
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



def mongodbizeMARC(raw_json):
    """Function takes raw JSON string, converts to hash-model for MARC JSON
    ingestion into MongoDB.

    Args:
        raw_json(str): Raw text string

    Returns:
        dict: Dictionary for ingestion into MongoDB
    """
    def convert_subfields(field):
        """Function converts a MARC JSON field into subfields

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

    record_dict = json.loads(raw_json)
    for row in record_dict['fields']:
            tag = row.keys()[0]
            contents = convert_subfields(row.get(tag))
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


def main():
    pass

if __name__ == '__main__':
    main()
