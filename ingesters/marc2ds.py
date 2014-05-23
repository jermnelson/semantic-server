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
import json
import pymarc
import os
import subprocess
import shutil
import socket
import sys
import urllib
import urllib2
import uuid

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

from __init__ import generate_record_info


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


class MARC21Ingester(object):

    def __init__(self, **kwargs):
        self.mongo_client = kwargs.get('mongo_client', None)

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

    def __get_or_add_marc__(self, record):
        """Internal method takes a MARC record and either returns an existing
        MARC Mongo ID or creates a new MARC entity in the semantic server

        Args:
            record (pymarc.Record): MARC21 record

        Returns:
            str: String of MARC records MongoDB ID
        """
        for field in record.get_fields('035'):
            if 'a' in field.subfields:
                mongod_id = self.mongo_client.marc.bibliographic.find_one(
                    {"fields.035.subfields.a": field['a']},
                    {"_id"})
                if mongod_id is not None:
                    return str(mongod_id)
        mongod_id = self.__convert_fields_add_datastore__(record.as_dict())
        return mongod_id

class MARC21toBIBFRAMEIngester(MARC21Ingester):
    """
    Class ingests MARC21 records through
    Library of Congress's MARC2BIBFRAME xquery framework using Saxon


    >> mongo_client = mongo_client=MongoClient()
    >> ingester = MARC21toBIBFRAMEIngester(marc21='test.mrc',
                                           mongo_client=mongo_client)
    >> ingester.run()
    """
    AUTH_ACCESS_PT = URIRef("http://bibframe.org/vocab/authorizedAccessPoint")
    COLLECTION_CLASSES = {
         'v1#Authority': "Authority", # MADS mapping
         'v1#ComplexSubject': 'Topic', # MADS mapping
         'v1#ConferenceName': 'Authority', # MADS mapping
         'v1#CorporateName': 'Organization', # MADS mapping
         'v1#GenreForm': 'Annotation', # MADS mapping
         'v1#Geographic': 'Topic', # MADS mapping
         'v1#PersonalName': 'Person', # MADS mapping
         'v1#NameTitle': 'Authority', # MADS mapping
         'v1#Topic': 'Topic', # MADS mapping
         'Archival': 'Instance',
         'Audio': 'Work',
         'Cartography': 'Work',
         'Collection': 'Instance',
         'Dataset': 'Work',
         'Electronic': 'Instance',
         'Family': 'Authority',
         'HeldMaterial': 'Annotation',
         'Integrating': 'Instance',
         'Jurisdiction': 'Authority',
         'Manuscript': 'Instance',
         'Meeting': 'Authority',
         'MixedMaterial': 'Work',
         'Monograph': 'Instance',
         'MovingImage': 'Work',
         'Multimedia': 'Work',
         'MultipartMonograph': 'Instance',
         'NotatedMovement': 'Work',
         'NotatedMusic': 'Work',
         'Place': 'Authority',
         'Print': 'Instance',
         'Review': 'Annotation',
         'Serial': 'Instance',
         'StillImage': 'Work',
         'Summary': 'Annotation',
         'TableOfContents': 'Annotation',
         'Tactile': 'Instance',
         'Text': 'Work',
         'ThreeDimensionalObject': 'Work'}

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
        self.baseuri = kwargs.get('baseuri')
        self.saxon_jar_location = kwargs.get('jar_location', None)
        self.saxon_xqy_location = kwargs.get('xqy_location', None)
        self.xquery_host = kwargs.get('xquery_host', 'localhost')
        self.xquery_port = kwargs.get('xquery_port', 8089)
        self.graph_ids = {}
        self.filenames = []
        self.language_labels = {}
        super(MARC21Ingester, self).__init__(
            mongo_client=kwargs.get('mongo_client'))

    def __add_entity__(self, subject, graph, collection=None, marc_id=None):
        """Internal method takes a URIRef and a graph, expands any URIRefs and
        then adds entity to Semantic Server. If collection is None, attempts to
        guess collection.

        Args:
            subject (rdflib.URIRef): BIBFRAME subject
            graph (rdflib.Graph): BIBFRAME graph
            collection (pymongo.Collection): Semantic server bibframe collection
            marc_id (str): String of MARC Semantic server Mongo ID

        Returns:
            ObjectID: Entity's MongoDB ID
        """

        def add_or_extend_property(name, value):
            if name in doc:
                if type(doc[name]) != list:
                    doc[name]= [doc[name], value]
                else:
                    if value not in doc[name]:
                        doc[name].append(value)
            else:
                doc[name] = value

        doc = {'@id': str(subject),
               '@type': []}
        if collection is None:
            collection = self.__get_collection__(subject, graph)
        # Iterates through predicates and objects for the subject, expanding
        # some predicates or creating new entities in others
        id_value_uri = 'http://bibframe.org/vocab/identifierValue'
        id_audience_uri = 'http://bibframe.org/vocab/Audience'
        id_uri_uri = 'http://bibframe.org/vocab/uri'
        doc['@type'].append(self.__get_type__(subject, graph))
        for predicate, obj in graph.predicate_objects(subject=subject):
            if type(predicate) == URIRef:
                if predicate.startswith('http://bibframe'):
                    bf_property = predicate.split("/")[-1]
                    if type(obj) == Literal:
                        doc[bf_property] = obj.value
                    elif type(obj) == URIRef:
                        # Gets literal if object's type is Identifier
                        object_type = self.__get_type__(obj, graph)

                        if object_type.startswith('Category'):
                            add_or_extend_property(
                                bf_property,
                                self.__get_or_add_category__(
                                    obj,
                                    graph))
                        elif object_type.lower().startswith('classification'):
                            expanded_class = self.__expand_classification__(
                                    classification=obj,
                                    graph=graph)
                            class_key = expanded_class.keys()[0]
                            if bf_property in doc:
                                doc[bf_property][class_key] = expanded_class[
                                    class_key]
                            else:
                                doc[bf_property] = {
                                    class_key: expanded_class.get(class_key)}
                        elif bf_property.startswith('classification'):
                            if str(obj).startswith('http://id.loc.gov'):
                                # Get last part of id.loc.gov (may not reference
                                # anything

                                add_or_extend_property(
                                    bf_property,
                                    obj.split("/")[-1])
                        elif object_type.startswith('Identifier'):
                            object_value = graph.value(
                                subject=obj,
                                predicate=URIRef(id_value_uri))
                            add_or_extend_property(
                                bf_property,
                                object_value.value)
                        elif bf_property.startswith("isbn"):
                            add_or_extend_property(
                                bf_property,
                                obj.split("/")[-1])
                        elif bf_property.startswith('language'):
                            add_or_extend_property(
                                bf_property,
                                self.__process_language__(obj))
                        elif str(predicate) == id_uri_uri:
                            add_or_extend_property(bf_property, obj.decode())
                        elif obj.startswith('http://www.loc.gov/mads/'):
                            continue
                        else:
                            add_or_extend_property(
                                bf_property,
                                str(self.__get_or_add_entity__(
                                    obj,
                                    graph,
                                    marc_id)))
        if 'derivedFrom' in doc and marc_id is not None:
            doc['derivedFrom'] = marc_id
        entity_id = collection.insert(doc)
        self.graph_ids[str(subject)] = str(entity_id)
        return entity_id



    def __expand_classification__(self, classification, graph):
        """Internal method takes a classification subject and expands the
        graph into a dictionary.

        Args:
            classification (rdflib.URIRef|rdflib.BNode): BIBFRAME subject
            graph (rdflib.Graph): BIBFRAME graph

        Returns:
            dict: A dictionary of classification properties
        """
        output = {}
        schema_uri = URIRef('http://bibframe.org/vocab/classificationScheme')
        schema = graph.value(
            subject=classification,
            predicate=schema_uri)
        if schema is not None:
            schema_val = str(schema)
            output[schema_val] = {'classificationScheme': schema_val}
        else:
            schema_val = str(classification)
            output[schema_val] = {}
        for pred, obj in graph.predicate_objects(subject=classification):
            if pred == schema_uri or pred == self.RDF_TYPE_URI:
                continue
            output[schema_val][pred.split("/")[-1]] = str(obj)
        return output


    def __get_collection__(self, subject, graph):
        """Interal method takes a URIRef and a graph, and depending on the rdf
        type, returns the Mongo DB Collection for the Bibframe entity

        Args:
            subject (rdflib.URIRef): BIBFRAME subject
            graph (rdflib.Graph): BIBFRAME graph

        Returns:
            pymongo.Collection: Collection for the subject
        """
        type_of =  self.__get_type__(subject, graph)
        if type_of in MARC21toBIBFRAMEIngester.COLLECTION_CLASSES:
            name = MARC21toBIBFRAMEIngester.COLLECTION_CLASSES.get(type_of)
            collection = getattr(self.mongo_client.bibframe,
                                 name)
        else:
            collection = getattr(self.mongo_client.bibframe,
                                 type_of)
        return collection

    def __get_or_add_category__(self, category, graph):
        """Internal method takes a category subject and a BIBFrame graph and
        either returns existing category ID or returns a new category

        Args:
            category (rdflib.URIRef): URIRef of catagory
            graph (rdflib.Graph): BIBFRAME graph

        Returns:
            str: String MongoDB ID of category
        """
        category_type, category_value = None, None
        if self.__get_type__(category, graph) != 'Category':
            return # Not a category returns nothing
        categoryType = graph.value(
            subject=category,
            predicate=URIRef('http://bibframe.org/vocab/categoryType'))
        if categoryType is not None:
            category_type = str(categoryType)
        categoryValue = graph.value(
            subject=category,
            predicate=URIRef('http://bibframe.org/vocab/categoryValue'))
        if categoryValue is not None:
            category_value = str(categoryValue)
        # Must match both type and value otherwise create new Category
        category_id = self.mongo_client.bibframe.Category.find_one({
            'categoryType': category_type,
            'categoryValue': category_value},
            {"_id": 1})
        if not category_id:
           category_id = self.__add_entity__(category, graph)
        return str(category_id)

    def __get_or_add_entity__(self, subject, graph, marc_id=None):
        """Internal method takes a URIRef and a graph, first checking to see
        if the subject already exists as an entity in the datastore or creates
        a entity by iterating through the subject's predicates and objects

        Args:
            subject (rdflib.URIRef): BIBFRAME subject
            graph (rdflib.Graph): BIBFRAME graph
            marc_id (str): Semantic server's id for MARC record

        Returns:
            str: String of entity's MongoDB ID
        """
        bibframe_type = self.__get_type__(subject, graph)
        # Filters out certain fields w/specific methods
        if ["Title"].count(bibframe_type):
            return self.__process_title__(subject, graph)
        collection = self.__get_collection__(subject, graph)
        result = collection.find_one(
                    {"@id": str(subject)},
                    {"_id"})
        authorized_access_point = graph.value(
            subject=subject,
            predicate=URIRef(
                u'http://bibframe.org/vocab/authorizedAccessPoint'))
        if authorized_access_point is not None:
            result = collection.find_one(
                {"authorizedAccessPoint": authorized_access_point.value},
                {"_id":1})
        # Finally searches label

        if result is not None:
            return str(result.get('_id'))
        # Doesn't exist in collection, now adds entity
        return str(self.__add_entity__(subject, graph, collection, marc_id))





    def __get_type__(self, subject, graph):
        """Internal method takes a subject and a graph, tries to extract
        type, return BIBFRAME class or most generic Resource type

        Args:
            subject (rdflib.URIRef): BIBFRAME subject
            graph (rdflib.Graph): BIBFRAME graph

        Returns:
            str: BIBFRAME class
        """
        bibframe_type_object = graph.value(
            subject=subject,
            predicate=URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'))
        if bibframe_type_object is not None:
            bibframe_type = bibframe_type_object.split("/")[-1]
            return bibframe_type
        return 'Resource' # Most general BIBFRAME type if no match is made


    def __mongodbize_graph__(self, graph, marc_db_id=None):
        """Internal method takes BIBFRAME rdflib.Graph and ingests into MongoDB

        Args:
            graph (rdflib.Graph): BIBFRAME graph
            marc_db_id (str): String of MARC21 MongoDB ID

        Returns:
            str: String of MongoDB ID of primary BIBFRAME Work
        """
##        titles = self.__process_titles__(graph)
##        instances = self.__process_instances__(graph)
        for subject in graph.subjects():
            if not str(subject) in self.graph_ids:
                self.__get_or_add_entity__(subject, graph, marc_db_id)


    def __process_subject__(self, subject, graph):
        def get_str_or_list(name, value):
            if name in doc:
                if type(doc[name]) == list:
                    doc[name].append(value)
                else:
                    doc[name] = [doc[name],]
            else:
                doc[name] = value
        doc = {}
        for p,o in graph.predicate_objects(subject):
            if p == MARC21toBIBFRAMEIngester.RDF_TYPE_URI:
                doc['@type'] = o.split("/")[-1]
            if str(p).startswith('http://bibframe'):
                bf_property = p.split("/")[-1]
                if type(o) == rdflib.Literal:
                    get_str_or_list(bf_property, str(o))
                elif [rdflib.BNode, rdflib.URIRef].count(type(o)):
                    get_str_or_list(bf_property, self.__process_subject__(o))
        collection = self.__get_collection__(subject, graph)
        subject_id = collection.insert(doc)
        self.graph_ids[str(subject)] = subject_id
        return str(subject_id)

    def __process_instances__(self, graph):
        """Internal method takes BIBFRAME graph extracts Instances, and returns
        a dictionary of graph URIs with Mongo IDs from the Semantic Server.

        Args:
            graph (rdflib.Graph): BIBFRAME graph

        Returns:
            dict: Dictionary of graph subject URIs mapped to Mongo IDs
        """
        output = {}
        for subject in graph.subjects():
            subject_type = self.__get_type__(subject, graph)
            if subject_type == 'Instance':
                output[str(subject)] = self.__get_or_add_entity__(subject, graph)
            else:
                name = MARC21toBIBFRAMEIngester.COLLECTION_CLASSES.get(
                            subject_type, '')
                if name.startswith('Instance'):
                    output[str(subject)] = self.__get_or_add_entity__(
                                                subject,
                                                graph)
        return output

    def __process_language__(self, language):
        """Internal method takes a language URIRef, attempts retrival of the
        language label.

        Args:
            language (rdflib.URIRef): Language URIRef

        Returns:
            dict: Dictionary with uri and label
        """
        authoritativeLabel = "http://www.loc.gov/mads/rdf/v1#authoritativeLabel"
        uri = str(language)
        output = {'@id': uri}
        if uri in self.language_labels:
            return self.language_labels.get(uri)
        try:
            lang_json = json.load(urllib2.urlopen("{}.json".format(uri)))
            if lang_json is not None and authoritativeLabel in lang_json[0]:
                for lang in lang_json[0][authoritativeLabel]:
                    if lang.get('@language','').startswith('en'):
                        output['label'] = lang['@value']
            self.language_labels[uri] = output
        except (ValueError, urllib2.HTTPError):
            # URL not found or JSON malformed
            pass
        return output


    def __process_title__(self, title, graph):
        """Internal method takes title subject and a BIBFRAME graph and either
        returns an existing title or adds the title to the Semantic Server

        Args:
            graph (rdflib.Graph): BIBFRAME graph

        Returns:
            str: String of MongoDB ID of title
        """
        def get_title_property(subject, predicate):
            if type(predicate) != URIRef:
                predicate = URIRef(predicate)
            title_property = graph.value(
                                subject=subject,
                                predicate=predicate)
            if title_property is not None:
                if type(title_property) == Literal:
                    return title_property.value
            return str()
        auth_access_pt = get_title_property(
            title,
            MARC21toBIBFRAMEIngester.AUTH_ACCESS_PT)
        title_value = get_title_property(
                        title,
                        'http://bibframe.org/vocab/titleValue')
        sub_title = get_title_property(
                        title,
                        'http://bibframe.org/vocab/subtitle')
        label = get_title_property(
                    title,
                    'http://bibframe.org/vocab/label')

        # First try authorized Access Point
        title_result = self.mongo_client.bibframe.Title.find_one(
            {"authorizedAccessPoint": auth_access_pt},
            {"_id":1})
        # Second try titleValue and subtitle
        if title_result is None:
            title_result = self.mongo_client.bibframe.Title.find_one(
                {"$and": [{"titleValue": title_value},
                          {"subtitle": sub_title}]
                },
                { "_id": 1})
        # Third try label
        if title_result is None:
            title_result = self.mongo_client.bibframe.Title.find_one(
                {"label": label},
                {"_id":1})
        # Forth try title id
        if title_result is None:
            title_result = self.mongo_client.bibframe.Title.find_one(
                {"@id": str(title)},
                {"_id":1})
        # Fifth tries just titleValue
        if title_result is None:
            title_result = self.mongo_client.bibframe.Title.find_one(
                {"titleValue": title_value},
                {"_id":1})
        # Finally add Title if no matches
        if title_result is None:
            title_id = self.__add_entity__(
                title,
                graph,
                self.mongo_client.bibframe.Title)
        else:
            title_id = title_result.get('_id')
        return title_id


    def __process_titles__(self, graph):
        """Internal method takes BIBFRAME rdflib graph, extracts titles and
        either returns an existing Mongo ID from the Semantic Server or adds the
        title to the Semantic Server.

        Args:
            graph (rdflib.Graph): BIBFRAME graph

        Returns:
            dict: Dictionary of Mongo ObjectIDs by title URI
        """
        output = {}


        for title in graph.subjects(
            predicate = MARC21toBIBFRAMEIngester.RDF_TYPE_URI,
            object=URIRef(u'http://bibframe.org/vocab/Title')):
                 output[str(title)] = self.__process_title__(title, graph)


##                title_result = self.mongo_client.bibframe.Title.find_one(
##                    { '$or': [
##                        {"authorizedAccessPoint": auth_access_pt},
##                        {"$and": [{"titleValue": title_value},
##                                  {"subtitle": sub_title}]
##                        }
##                        ]
##                    },
##                    { "_id": 1})
        return output


    def __xquery_chain__(self, marc_xml):
        xquery_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        xquery_server.connect((self.xquery_host, self.xquery_port))
        xquery_server.sendall(marc_xml + "\n")
        rdf_xml = ''
        while 1:
            data = xquery_server.recv(1024)
            if not data:
                break
            rdf_xml += data
        xquery_server.close()
        bf_graph = Graph()
        bf_graph.parse(data=rdf_xml, format='xml')
        return bf_graph

    def __xquery_chain__process__(self, marc_xml):
        """Internal method takes a MARC XML document, serializes to temp
        location, runs a Java subprocess with Saxon to convert from MARC XML to
        a temp RDF XML version and then returns a BIBFRAME graph.

        Args:
            marc_xml (etree.XML): XML of MARC record

        Returns:
            rdflib.Graph:  BIBFRAME graph of rdf xml from parsed xquery
        """
        xml_file = NamedTemporaryFile(delete=False)
        xml_file.write(marc_xml)
        xml_file.close()
        xml_filepath = r"{}".format(xml_file.name).replace("\\","/")
        java_command = ['java',
                     '-cp',
                     self.saxon_jar_location,
                     'net.sf.saxon.Query',
                     self.saxon_xqy_location,
                     'marcxmluri={}'.format(xml_filepath),
                     'baseuri={}'.format(self.baseuri),
                     'serialization=rdfxml']
        process = subprocess.Popen(java_command,
                                   stdout=subprocess.PIPE)
        raw_bf_rdf, err = process.communicate()
        bf_graph = Graph()
        bf_graph.parse(data=raw_bf_rdf, format='xml')
        os.remove(xml_file.name)
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
        self.graph_ids = {}
        # MARC record must have a 001 for BIBFRAME xquery to function properly
        if not record['001']:
            unique_id = uuid.uuid1()
            field001 = pymarc.Field('001')
            field001.data = str(unique_id).split("-")[0]
            record.add_field(field001)
        marc_id = self.__get_or_add_marc__(record)
##        marc_xml = etree.XML(pymarc.record_to_xml(record, namespace=True))
        # if a BIBFRAME Work exists for the MARC record, skip query
        result = self.mongo_client.bibframe.Work.find_one(
            {"derivedFrom": marc_id},
            {"_id":1})
        if result is None:
            marc_xml = pymarc.record_to_xml(record, namespace=True)

            bibframe_graph = self.__xquery_chain__(marc_xml)
            self.__mongodbize_graph__(bibframe_graph, marc_id)
            bibframe_graph.close()


class MARC21toSchemaOrgIngester(MARC21Ingester):
    """
    Class ingests MARC21 records through OCLC xISBN and other web services to
    extract schema.org metadata based on the ISBN or other identifiers into
    Catalog Pull Platform's Semantic Server.

    >> mongo_client = mongo_client=MongoClient()
    >> ingester = MARC21toSchemaOrgIngester(marc21='test.mrc',
                                            mongo_client=mongo_client)
    >> ingester.run()
    """
    OCLC_XISBN_BASE = 'http://xisbn.worldcat.org/webservices/xid/'
    OCLC_XISSN_BASE = 'http://xissn.worldcat.org/webservices/xid/'
    OCLC_EXP_BASE = 'http://experiment.worldcat.org/entity/work/data/'
    COLLECTION_CLASSES = {}

    def __init__(self, **kwargs):
        self.mongo_client = kwargs.get('mongo_client', MongoClient())

    def __get_oclc_owi__(self, oclcnum):
        params = {
            'fl': '*',
            'format': 'json',
            'method': 'getMetadata'}
        url = urllib2.urlparse.urljoin(
                 MARC21toSchemaOrgIngester.OCLC_XISBN_BASE,
                 'oclcnum/')
        url = urllib2.urlparse.urljoin(url, str(oclcnum))
        oclc_json = json.load(urllib2.urlopen(
                                  url,
                                  data=urllib.urlencode(params)))
        owi = None
        for row in oclc_json.get('list'):
            if 'owi' in row:
                owi = row.get('owi')
                break
        if len(owi) == 1:
            return owi[0]
        else:
            return owi # Returns a list


    def __get_or_add_oclc_creator__(self, creators):
        creators = []
        loc_urls = []
        for url in creators:
            creator_id = self.mongo_client.schema.Person.find_one(
                {"sameAs": url})
            if creator_id is not None:
                creators.append(str(creator_id))
            elif url.startswith("http://id.loc"):
                pass




    def __add_loc_name_authority__(self, loc_url):
        name_rec = jsons.load(urllib2.urlopen(loc_url))



    def __get_or_add_oclc_work__(self, oclc_json):
        """Internal method takes a oclc json for a work and either returns
        an existing semantic server ID for the schema.org/CreativeWork or
        creates a new schema.org/CreativeWork or CreativeWork subclass

        Args:
            oclc_json (dict): OCLC CreativeWork JSON from OCLC webservice
        """
        doc = {}
        if 'creator' in oclc_json:
            doc['creator'] = self.__get_or_add_oclc_creator__(
                oclc_json.get('creator'))





    def __get_type__(self, record):
        """Internal method takes a MARC21 record and attempts to guess
        schema.org type.

        Args:
            record (pymarc.Record): MARC21 record

        Returns:
            str: schema.org class
        """
        # Default
        return 'Thing'

    def __oclc_workflow__(self, record):
        """Internal method takes either an ISBN or ISSN, queries
        OCLC web services and returns the MongoDB id of the schema.org
        Creative Work.

        Args:
            record (pymarc.Record): MARC21 record

        Returns:
            str: String of MongoDB id of the schema.org/CreativeWork
        """
        def create_url(url_base, type_of, value):
            url = urllib2.urlparse.urljoin(url_base, "{}/".format(type_of))
            return urllib2.urlparse.urljoin(url, value)
        if record.isbn():
            url = create_url(MARC21toSchemaOrgIngester.OCLC_XISBN_BASE,
                             'isbn',
                             record.isbn())
        elif record['020'] is not None:
            # First start w/valid ISSN
            if record['020']['a'] is not None:
                url = create_url(MARC21toSchemaOrgIngester.OCLC_XISSN_BASE,
                                 'issn',
                                 record['020']['a'])
                url = urllib2.urlparse.urljoin(url, record['020']['a'])
            elif record['020']['m'] is not None:




    def __process_isbn__(self, isbn):
        url = urllib2.urlparse.urljoin(
            MARC21toSchemaOrgIngester.OCLC_XISBN_BASE,
            'isbn/')
        url = urllib2.urlparse.urljoin(url, isbn)
        params = {
            'fl': 'oclcnum',
            'format': 'json',
            'method': 'getMetadata'}
        oclc_json = json.load(
            urllib2.urlopen(
                url,
                data=urllib.urlencode(params)))
        for row in oclc_json.get('list'):
            if 'oclcnum' in row:
                oclcnums = row.get('oclcnum')
                break
        if oclcnums is not None:
            for number in oclcnums:
                owi = self.__get_oclc_owi__(number)
                self.__process_owi__(owi)

    def __process_owi__(self, owi):
        if type(owi) == str:
            owi = owi.replace("owi", '') # Remove owi prefix
        url = urllib2.urlparse.urljoin(
            MARC21toSchemaOrgIngester.OCLC_EXP_BASE,
            "{}.jsonls".format(owi))
        owi_json = jsons.load(urllib2.urlopen(url))
        graph = owi_json.get('@graph')
        for row in graph:
            if row.get('@id').endswith("jsonld"):
                continue
            work_id = self.__get_or_add_oclc_work__(row)







    def batch(self, marc_filename):
        pass


    def ingest(self, record):
        """Method runs entire tool-chain to ingest a single MARC record into
        Semantic Server's schema.org database and collections.

        Args:
            record (pymarc.Record): MARC21 record
        """
        if record.isbn():
            work_id = self.__work_harvest__(record)








