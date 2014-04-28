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
from pymongo import MongoClient
from rdflib import BNode, Graph, plugin, Literal, URIRef

from tempfile import NamedTemporaryFile
try:
    from lxml import etree
except ImportError:
    import xml.etree.ElementTree as etree

import flask_bibframe.models as bf_models

from bson import ObjectId
import flask_schema_org.models as schema_models

sys.path.append("E:/tiger-catalog/")

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
    AUTH_ACCESS_PT = URIRef("http://bibframe.org/vocab/authorizedAccessPoint")
    RDF_TYPE_URI = URIRef(u'http://www.w3.org/1999/02/22-rdf-syntax-ns#type')

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
        self.mongo_client = kwargs.get('mongo_client', MongoClient())
        self.saxon_jar_location = kwargs.get('jar_location', None)
        self.saxon_xqy_location = kwargs.get('xqy_location', None)
        self.graph_ids = {}


    def __add_entity__(self, subject, graph, collection=None):
        """Internal method takes a URIRef and a graph, expands any URIRefs and
        then adds entity to Semantic Server. If collection is None, attempts to
        guess collection.

        Args:
            subject (rdflib.URIRef): BIBFRAME subject
            graph (rdflib.Graph): BIBFRAME graph

        Returns:
            ObjectID: Entity's MongoDB ID
        """
        doc = {}
        if type(subject) == BNode:
            return
        if collection is None:
            pass #! Need to implement
        # Iterates through predicates and objects for the subject, expanding
        # some predicates or creating new entities in others
        id_value_uri = u'http://bibframe.org/vocab/identifierValue'
        id_audience_uri = 'http://bibframe.org/vocab/Audience'
        for predicate, obj in graph.predicate_objects(subject=subject):
            if type(predicate) == URIRef:
                if predicate.startswith('http://bibframe'):
                    bf_property = predicate.split("/")[-1]
                    if type(obj) == Literal:
                        doc[bf_property] = obj.value
                    elif type(obj) == URIRef:
                        # Gets literal if object's type is Identifier
                        object_type = self.__get_type__(obj, graph)
                        if object_type == 'Identifier' :
                            object_value = graph.value(
                                subject=obj,
                                predicate=URIRef(id_value_uri))
                            doc[bf_property] = object_value.value
                        else:
                            doc[bf_property] = self.__get_or_add_entity__(
                                obj,
                                graph)
        entity_id = collection.insert(doc)
        self.graph_ids[str(subject)] = str(entity_id)
        return entity_id

    def __convert_fields_add_datastore__(self, record_dict):
        """Internal method coverts pymarc MARC21 record to MongoDB optimized
        document for storage

        Args:
            record_dict (dict): Dictionary of a MARC21 record

        Returns:
            str: MongoDB Identifier
        """
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

    def __get_or_add_entity__(self, subject, graph):
        """Internal method takes a URIRef and a graph, first checking to see
        if the subject already exists as an entity in the datastore or creates
        a entity by iterating through the subject's predicates and objects

        Args:
            subject (rdflib.URIRef): BIBFRAME subject
            graph (rdflib.Graph): BIBFRAME graph

        Returns:
            str: String of entity's MongoDB ID
        """
        bibframe_type = self.__get_type__(subject, graph)
        print(bibframe_type, self.mongo_client.bibframe.collection_names())
        if not bibframe_type in self.mongo_client.bibframe.collection_names():
            pass #! Need to add logic here, could be a class dict lookup
        else:
            collection = getattr(self.mongo_client.bibframe, bibframe_type)
        authorized_access_point = graph.value(
            subject=subject,
            predicate=URIRef(
                u'http://bibframe.org/vocab/authorizedAccessPoint'))
        if authorized_access_point is not None:
            result = collection.find_one(
                {"authorizedAccessPoint": authorized_access_point.value},
                {"_id":1})
            if result is not None:
                return str(result.get('_id'))
        # Doesn't exist in collection, now adds entity
        return self.__add_entity__(subject, graph, collection)

    def __get_type__(self, subject, graph):
        bibframe_type_object = graph.value(
            subject=subject,
            predicate=URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'))
        bibframe_type = bibframe_type_object.split("/")[-1]
        return bibframe_type


    def __mongodbize_graph__(self, graph, marc_db_id=None):
        """Internal method takes BIBFRAME rdflib.Graph and ingests into MongoDB

        Args:
            graph (rdflib.Graph): BIBFRAME graph
            marc_db_id (str): String of MARC21 MongoDB ID

        Returns:
            str: String of MongoDB ID of primary BIBFRAME Work
        """
        titles = self.__process_titles__(graph)
        for subject in graph.subjects():
            if type(subject) == BNode: # For now ignoring all BNode
                continue

    def __process_instance__(self, graph):
        """Internal method takes BIBFRAME graph extracts Instances, and returns
        either existing Mongo ID or list of Mongo IDs from the Semantic Server.

        Args:
            graph (rdflib.Graph): BIBFRAME graph

        Returns:
            ObjectId: If there is only 1 Instance
            list: List of ObjectIds
        """
        pass



    def __process_titles__(self, graph):
        """Internal method takes BIBFRAME rdflib graph, extracts titles and
        either returns an existing Mongo ID from the Semantic Server or adds the
        title to the Semantic Server.

        Args:
            graph (rdflib.Graph): BIBFRAME graph

        Returns:
            list: List of Mongo ObjectIDs for the title
        """
        output = []
        def get_title_property(subject, predicate):
            if type(predicate) != URIRef:
                predicate = URIRef(predicate)
            title_property = graph.value(
                                subject=subject,
                                predicate=predicate)
            if title_property is not None:
                if type(title_property) == Literal:
                    return title_property.value

        for title in graph.subjects(
            predicate = RDF_TYPE_URI,
            object=URIRef(u'http://bibframe.org/vocab/Title')):
                auth_access_pt = get_title_property(
                                    title,
                                    AUTH_ACCESS_PT)
                title_value = get_title_property(
                                title,
                                u'http://bibframe.org/vocab/titleValue')
                sub_title = get_title_property(
                                title,
                                u'http://bibframe.org/vocab/subtitle')
                title_id = self.mongo_client.bibframe.Title.find_one(
                    { '$or': [
                        {"authorizedAccessPoint": auth_access_pt},
                        {"$and": [{"titleValue": title_value},
                                  {"subtitle": sub_title}]
                        }
                        ]
                    },
                    { "_id": 1})
                if title_id is None:
                    title_id = self.__add_entity__(
                        title,
                        self.mongo_client.bibframe.Title)
                output.append(title_id)
        return output





    def __xquery_chain__(self, marc_xml):
        """Internal method takes a MARC XML document, serializes to temp
        location, runs a Java subprocess with Saxon to convert from MARC XML to
        a temp RDF XML version and then returns a BIBFRAME graph.

        Args:
            marc_xml (etree.XML): XML of MARC record

        Returns:
            rdflib.Graph:  BIBFRAME graph of rdf xml from parsed xquery
        """
        xml_file = NamedTemporaryFile(delete=False)
        xml_file.write(etree.tostring(marc_xml))
        xml_file.close()
        xml_filepath = r"{}".format(xml_file.name).replace("\\","/")
        process = subprocess.Popen(['java',
                     '-cp',
                     self.saxon_jar_location,
                     'net.sf.saxon.Query',
                     self.saxon_xqy_location,
                     'marcxmluri={}'.format(xml_filepath),
                     'baseuri={}'.format(self.baseuri),
                     'serialization=rdfxml'],
                                   stdout=subprocess.PIPE)
        raw_bf_rdf, err = process.communicate()
        bf_graph = Graph()
        bf_graph.parse(data=raw_bf_rdf, format='xml')
        return bf_graph



    def batch(self, marc_filepath):
        marc_reader = pymarc.MARCReader(open(marc_filepath),
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
            self.ingest(record)
        end_time = datetime.datetime.utcnow()



    def ingest(self, record):
        """Method runs entire tool-chain to ingest a single MARC record into
        Datastore.

        Args:
            record (pymarc.Record): MARC21 record

        Returns:
            list: MongoID
        """
        marc_id = self.__convert_fields_add_datatsore__(record.as_dict())
        marc_xml = etree.XML(pymarc.record_to_xml(record, True))
        bibframe_graph = self.__xquery_chain__(marc_xml)



