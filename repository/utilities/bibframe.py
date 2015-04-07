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
from elasticsearch import Elasticsearch
from .namespaces import *


def guess_search_doc_type(graph, fcrepo_uri):
    """Function takes a graph and attempts to guess the Doc type for ingestion
    into Elastic Search

    Args:
        graph(rdflib.Graph): RDF Graph of Fedora Object
        subject_uri(rdlib.URIRef): Subject of the RDF Graph

    Returns:
        string: Doc type of subject
    """
    doc_type = 'Resource'
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
    return doc_type

def get_base_url(graph):
    for name in ['bf:Work', 'bf:Instance', 'bf:Person']:
        sparql = """PREFIX rdf: <{0}>
PREFIX bf: <{1}>
SELECT ?subject 
WHERE {{
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
        self.base_url = get_base_url(self.graph) 
        self.subjects = subjects_list(self.graph, self.base_url)
        self.dedup_predicates.extend([ 
         BF.authorizedAccessPoint, 
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

 
    def __process_subject__(self, row):
        subject, graph = row
        bf_type = self.__get_specific_type__(subject)
        existing_uri = self.searcher.triplestore.__sameAs__(str(subject))
        if existing_uri:
            subject = rdflib.URIRef(existing_uri)
        fedora_url, new_graph = self.__add_or_get_graph__(
            subject=subject, 
            graph_type=bf_type,
            doc_type=guess_search_doc_type(graph, subject),
            index='bibframe')
        subject_uri = rdflib.URIRef(fedora_url)
        return subject_uri

    def __clean_up__(self):
        """Internal method performs update on all subjects of the graph, updating
        all internal subject URIs to their corresponding Fedora 4 URIs. """
        for subject, graph in self.subjects:
            pass

         
class BIBFRAMESearch(Search):
    SUGGESTION_TYPES = [
        BF.Work, 
        BF.Instance, 
        BF.Place, 
        BF.Organization, 
        BF.Title]

    def __init__(self, **kwargs):
        super(BIBFRAMESearch, self).__init__(**kwargs)


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
                add_suggestion = True
                break
        if add_suggestion:
            self.body['suggest'] = {
                "input": [],
                "output": "",
                "payload": {"id": doc_id}}

                                
        

def main():
    """Main function"""
    pass

if __name__ == '__main__':
    main()
