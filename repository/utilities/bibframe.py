"""
Name:        bibframe
Purpose:     Helper functions for ingesting BIBFRAME graphs into Fedora 4
             supported by Elastic Search

Author:      Jeremy Nelson

Created:     2014/11/02
Copyright:   (c) Jeremy Nelson 2014, 2015
Licence:     GPLv3
"""
__author__ = "Jeremy Nelson"

import base64
import datetime
import falcon
import json
import os
import rdflib
import re
import requests
import sys
import urllib.parse
import urllib.request
from .ingesters import default_graph, GraphIngester, subjects_list
from .. import CONTEXT, Search
from ..resources.fedora import Resource
from elasticsearch import Elasticsearch
from .namespaces import *
from .cover_art import by_isbn

COVER_ART_SPARQL = """PREFIX bf: <{}>
PREFIX rdf: <{}>
SELECT DISTINCT ?cover
WHERE {{
  ?cover rdf:type bf:CoverArt .
}}""".format(BF, RDF)

def guess_search_doc_type(graph, fcrepo_uri):
    """Function takes a graph and attempts to guess the Doc type for ingestion
    into Elastic Search

    Args:
        graph(rdflib.Graph): RDF Graph of Fedora Object
        subject_uri(rdlib.URIRef): Subject of the RDF Graph

    Returns:
        string: Doc type of subject
    """
    doc_type = None
    subject_types = [
        obj for obj in graph.objects(
            subject=fcrepo_uri,
            predicate=rdflib.RDF.type)
    ]
    for class_name in [
        'Work',
        'Annotation',
        'Authority',
        'HeldItem',
        'Person',
        'Place',
        'Provider',
        'Title',
        'Topic',
        'Organization',
        'Instance'
    ]:
        if getattr(BF, class_name) in subject_types:
            doc_type = class_name
            break
    if MADS.Authority in subject_types:
        doc_type = 'Authority'
    if not doc_type:
        doc_type = 'Resource'
    return doc_type

def get_base_url(graph):
    for name in ['bf:Work', 'bf:Instance', 'bf:Person']:
        sparql = """PREFIX rdf: <{0}>
PREFIX bf: <{1}>
SELECT ?subject 
WHERE {{http://bibframe.org/vocab/
    ?subject rdf:type {2} .
}}""" .format(RDF, BF, name)
        for subject in graph.query(sparql):
            if subject:
                url = urllib.parse.urlparse(str(subject[0]))
                return "{}://{}".format(url.scheme, url.netloc)


class Ingester(GraphIngester):

    def __init__(self, **kwargs):
        if not 'search' in kwargs:
            kwargs['search'] = BIBFRAMESearch(config=kwargs.get('config'))
        super(Ingester, self).__init__(**kwargs)
        if not 'base_url' in kwargs:
            self.base_url = get_base_url(self.graph) 
        self.subjects = subjects_list(self.graph, self.base_url)
        self.dedup_predicates.extend([ 
         BF.authorizedAccessPoint, 
         BF.identifierValue,
         BF.classificationNumber,
         BF.label, 
         BF.titleValue])
    
    def __get_specific_type__(self, subject):
        bf_type = self.__get_types__(subject, "http://bibframe", "bf")
        if len(bf_type) < 1:
            # General bf:Resource type as default
            return "bf:Resource"
        return bf_type[0]
   
    def __add_held_items__(self, **kwargs):
        """Helper method is intended to be overridden by Ingester children"""
        pass

    def __add_cover_art__(self, row):
        """Internal method attempts to retrieves cover art image for the 
        instance from one or more web services. If successful, adds new
        CoverArt resource to datastore.

        Args:
          row -- instance, graph tuple
        """ 
        cover_json = None
        instance, graph = row[0], row[1]
        # First test for bf:isbn10, bf:isbn12, and finally with general bf:isbn
        for predicate in [BF.isbn10, BF.isbn13, BF.isbn]: 
            isbn = graph.value(subject=instance, predicate=predicate)
            if isbn:
                value = str(isbn).split("/")[-1]
                cover_json = by_isbn(value)
                break
        if cover_json:
            if "bf:annotationBody" in cover_json:
                raw_image = cover_json.pop("bf:annotationBody")[0]['@value']
            if not "rdf:type" in cover_json:
                cover_json["rdf:type"] = str(BF.CoverArt)
            cover_json['bf:coverArtFor'] = [
                {"@id": self.searcher.triplestore.__sameAs__(str(instance))}]
            cover_art_graph = default_graph()
            cover_art_graph.parse(data=json.dumps(cover_json), 
                                  format='json-ld',
                                  context=CONTEXT)
            graph_ingester = GraphIngester(config=self.config, 
                                           graph=cover_art_graph,
                                           base_url=self.base_url)
            cover_art = Resource(self.config, self.searcher)
            # Currently default mimetype for these covers is image/jpeg
            cover_art_url = cover_art.__create__(
                rdf=cover_art_graph, 
                binary=raw_image, 
                mimetype='image/jpeg')
            
            
 
    def __process_subject__(self, row):
        subject, graph = row[0], row[1]
        bf_type = self.__get_specific_type__(subject)
        existing_uri = self.searcher.triplestore.__sameAs__(str(subject))
        if existing_uri:
            subject = rdflib.URIRef(existing_uri)
        
        fedora_url, new_graph = self.__add_or_get_graph__(
            subject=subject, 
            graph=graph,
            graph_type=bf_type)#,
            #doc_type=guess_search_doc_type(graph, subject),
            #index='bibframe')
        subject_uri = rdflib.URIRef(fedora_url)
        return subject_uri

    def __clean_up__(self):
        super(Ingester, self).__clean_up__()
        # Index into Elastic Search only after clean-up
        for row in self.subjects:
            if row[2] is False:
                continue
            subject = row[0]
            fedora_url = self.searcher.triplestore.__sameAs__(str(subject))
            fedora_uri = rdflib.URIRef(fedora_url)
            graph = default_graph()
            graph.parse(fedora_url)
            doc_type = guess_search_doc_type(graph, fedora_uri) 
            self.searcher.__index__(
                fedora_uri,
                graph, 
                doc_type, 
                'bibframe')

                    

         
class BIBFRAMESearch(Search):
    SUGGESTION_TYPES = [
        BF.Work, 
        BF.Instance, 
        BF.Person,
        BF.Place, 
        BF.Organization, 
        BF.Title,
        BF.Topic]

    def __init__(self, **kwargs):
        super(BIBFRAMESearch, self).__init__(**kwargs)

    def __generate_body__(self, graph, prefix=None):
        """Internal method overrides default Search body generator to 
        for additional index processing on specific types.

        Args:
            graph -- rdflib.Graph of BIBFRAME Resource
            prefix -- Prefix filter, will only index if object starts with a prefix,
                      default is None to index everything.
        """
        super(BIBFRAMESearch, self).__generate_body__(graph, prefix)
        # Add coverArt annotationBody as base64 encoded jpg to body 
        query = graph.query(COVER_ART_SPARQL)
        if len(query.bindings) > 0:
             cover_url = query.bindings[0]['?cover']
             image_url = str(cover_url).split(
                 "fcr:metadata")[0]
             image_result = requests.get(image_url)
             if image_result.status_code < 400:
                 raw_image = image_result.content
                 encoded_image = base64.b64encode(raw_image)
                 if 'bf:coverArt' in self.body:
                     self.body['bf:coverArt'].append(encoded_image)
                 else:
                     self.body['bf:coverArt'] = [encoded_image,]
                          


    def __generate_suggestion__(self, subject, graph, doc_id):
        """Internal method generates Elastic Search auto-suggestion
        for a selected number of BIBFRAME Classes including Instance,
        Work, Person, Place, Organization, Topic

        Args:
            subject -- RDF Subject
            graph -- rdflib.Graph
            doc_id -- document id to return
        """
        add_suggestion = False
        for type_of in graph.objects(subject=subject, predicate=RDF.type):
            if BIBFRAMESearch.SUGGESTION_TYPES.count(
                type_of):
                name = str(type_of).split("/")[-1].lower()
                suggest_field = "{}_suggest".format(name)
                add_suggestion = True
                break
        if add_suggestion:
            input_ = [str(obj) for obj in graph.objects(
                         subject=subject, 
                         predicate=BF.label)]
            for predicate in [BF.authorizedAccessPoint, 
                              BF.title,
                              BF.titleStatement,
                              BF.titleValue]:
                for obj in graph.objects(subject=subject, predicate=predicate):
                    if type(obj) == rdflib.Literal and obj.datatype == XSD.string:
                        input_.append(obj)
                    
            input_ =  list(set(input_))    
            self.body[suggest_field] = {
                "input": input_,
                "output": ' '.join(input_),
                "payload": {"id": doc_id}}

                                
        

def main():
    """Main function"""
    pass

if __name__ == '__main__':
    main()
