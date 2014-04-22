#-------------------------------------------------------------------------------
# Name:        marc2ds
# Purpose:     Takes MARC in JSON format and ingests into Schema.org and
#              BIBFRAME databases
#
# Author:      Jeremy Nelson
#
# Created:     2014/03/12
# Copyright:   (c) Jeremy Nelson, Colorado College 2014
# Licence:     MIT
#-------------------------------------------------------------------------------
import datetime
import pymarc
import os
import subprocess
import sys

from collections import OrderedDict
from rdflib import Graph, plugin

from tempfile import NamedTemporaryFile
try:
    from lxml import etree
except ImportError:
    import xml.etree.ElementTree as etree

import flask_bibframe.models as bf_models

from bson import ObjectId
import flask_schema_org.models as schema_models

from catalog.mongo_datastore import generate_record_info


def __get_fields_subfields__(marc_rec,
                             fields,
                             subfields,
                             unique=True):
    output = []
    for field in marc_rec.get('fields'):
        tag = field.keys()[0]
        if fields.count(tag) > 0:
            for row in field[tag]['subfields']:
                subfield = row.keys()[0]
                if subfields.count(subfield) > 0:
                    output.append(row[subfield])
    if unique:
        output = list(set(output))
    return output

def __get_basic__(marc):
    """Helper function takes MARC JSON and returns common metadata as a dict

    Args:
        marc: MARC21 file

    Returns:
        dict: Dictionary of common properties
    """
    output = {}
    output['name'] = __get_fields_subfields__(marc, ['245'], ['a', 'b'], False)


    return output




def add_movie(marc, client):
    """Function creates Schema.org's Movie (http://schema.org/Movie) and
    BIBFRAME MovingImage (http://bibframe.org/vocab/MovingImage) documents in
    the client's schema_org.CreativeWork and bibframe.Work collections.

    Parameters:
        marc: MARC in JSON format
        client: MongoDB client

    Returns:
        dict: Dictionary of schema_org and bibframe ids
    """
    ld_ids = {}
    bibframe = client.bibframe
    schema_org = client.schema_org
    movie = schema_models.Movie(**__get_basic__(marc))
    setattr(movie, '@type', 'Movie')
    moving_image = bf_models.MovingImage()
    setattr(moving_image, '@type', 'MovingImage')





    return ld_ids

def add_get_title_authority(authority_marc, client):
    " "
    bibframe = client.bibframe
    uniform_title = ''.join(__get_fields_subfields__(
        authority_marc,
        ['130'], ['a']))
    lccn = ''.join(__get_fields_subfields__(
        authority_marc,
        ['010'], ['a'])).replace(" ","")
    title = bibframe.Title.find_one({
        'authorizedAccessPoint': uniform_title})
    if title:
        return title.get('_id')
    title = bf_models.Title(authorizedAccessPoint=uniform_title,
                            identifier={'lccn': lccn},
                            titleSource=str(authority_marc.get('_id')),
                            titleValue=uniform_title)
    setattr(title, 'varientLabel', [])
    for varient_title in __get_fields_subfields__(
        authority_marc,
        ['430'],
        ['a']):
            title.varientLabel.append(varient_title)
    title_dict = title.as_dict()
    title_dict['recordInfo'] = generate_record_info(
        u'CoCCC',
        u'From MARC Authority record')
    return bibframe.Title.insert(title_dict)

def add_get_title_bibliographic(bib_marc, client):
    " "
    bibframe = client.bibframe
    title_str = ''.join(__get_fields_subfields__(['245'], ['a']))
    subtitle = ''.join(__get_fields_subfields__(['245'], ['b']))
    title_value = '{}{}'.format(title_str, subtitle)
    title = bibframe.Title.find_one({
        'titleValue': title_value})
    if title:
        return title.get('_id')
    title = bf_models.Title(titleSource=str(authority_marc.get('_id')),
                            titleValue=title_value)
    if len(subtitle) > 0:
        title.subtitle = subtitle
    title_dict = title.as_dict()
    title_dict['recordInfo'] = generate_record_info(
        u'CoCCC',
        u'From MARC Authority record')
    return bibframe.Title.insert(title_dict.as_dict())


class MARC21toBIBFRAMEIngester(object):
    """
    Class ingests MARC21 records through
    Library of Congress's MARC2BIBFRAME xquery framework using Saxon


    >> mongo_client = mongo_client=MongoClient()
    >> ingester = MARC21toBIBFRAMEIngester(marc21='test.mrc',
                                           mongo_client=mongo_client)
    >> ingester.run()
    """
    def __init__(self, **kwargs):
        """Creates an ingester instance for ingesting MARC21 records through
        Library of Congress's MARC2BIBFRAME xquery framework using Saxon

        Args:
            baseuri -- Base URI, defaults to http://catalog/
            marc21 -- MARC21 file
            mongo_client -- MongoDB client
            jar_location -- Complete path to Saxon jar file
            xqy_location -- Complete path to saxon.xqy from bibframe

        """
        self.baseuri = kwargs.get('baseuri', 'http://catalog/')
        self.marc21 = kwargs.get('marc21', None)
        self.mongo_client = kwargs.get('mongo_client', None)
        self.saxon_jar_location = kwargs.get('jar_location', None)
        self.saxon_xqy_location = kwargs.get('xqy_location', None)

    def __convert_fields__(self, record):
        """Internal method coverts pymarc MARC21 record to MongoDB optimized
        document for storage

        Args:
            record - MARC21 record

        Returns:
            str - MongoDB Identifier
        """
        record_dict = record.as_dict()
        new_fields = OrderedDict()
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
        datastore_id = self.mongo_client.marc.bibliographic.insert(record_dict)
        return str(datastore_id)


    def __convert_subfields__(self, field):
        """Internal method converts pymarc field's subfields to OrderedDict

        Args:
            field - dict of MARC21 field

        Returns:
            dict
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

    def __xquery_chain__(self, marc_xml):
        """Internal method takes a MARC XML document, serializes to temp
        location, runs a Java subprocess with Saxon to convert from MARC XML to
        a temp RDF XML version and then returns a BIBFRAME graph.

        Args:
            marc_xml -- Etree XML of MARC

        Returns:
            graph -- rdflib Graph with parsed xquery
        """
        xml_file = NamedTemporaryFile(delete=False)
        xml_file.write(etree.serialize(marc_xml))
        xml_file.close()
        rdf_xml_file = NamedTemporaryFile(delete=False)
        rdf_xml_file.close()
        subprocess.call([
            'java',
            '-cp',
            self.saxon_jar_location,
            self.saxon_xqy_location,
            'net.sf.saxon.Query',
            'marcxmluri={}'.format(xml_file.name),
            'baseuri={}'.format(self.baseuri),
            '-o:{}'.format(rdf_xml_file.name)])
        bf_graph = Graph()
        bf_graph.parse(rdf_xml_file.name, format='xml')



    def run(self):
        'Method runs entire tool-chain'
        marc_reader = pymarc.MARCReader(open(self.marc21),
            to_unicode=True)
        start_time = datetime.datetime.utcnow()
        for i,record in enumerate(marc_reader):
            if not i%10:
                sys.stderr.write(".")
            if not i%100:
                sys.stderr.write(" {} ".format(i))
            if not i%1000:
                sys.stderr.write(" {} seconds".format(
                    (datetime.datetime.utcnow()-start_time).seconds))
            marc_id = self.__convert_fields__(record)
            marc_xml = etree.XML(pymarc.record_to_xml(record, True))












def bibframe2DataStore(marc21record, client):
    bibframe = client.bibframe


