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
from .. import CONTEXT
from elasticsearch import Elasticsearch

AUTHZ = rdflib.Namespace("http://fedora.info/definitions/v4/authorization#")
BF = rdflib.Namespace("http://bibframe.org/vocab/")
DC = rdflib.Namespace("http://purl.org/dc/elements/1.1/")
FCREPO = rdflib.Namespace("http://fedora.info/definitions/v4/repository#")
FEDORA = rdflib.Namespace("http://fedora.info/definitions/v4/rest-api#")
FEDORACONFIG = rdflib.Namespace("http://fedora.info/definitions/v4/config#")
FEDORARELSEXT = rdflib.Namespace("http://fedora.info/definitions/v4/rels-ext#")
FOAF = rdflib.Namespace("http://xmlns.com/foaf/0.1/")
IMAGE = rdflib.Namespace("http://www.modeshape.org/images/1.0")
INDEXING = rdflib.Namespace("http://fedora.info/definitions/v4/indexing#")
MADS = rdflib.Namespace("http://www.loc.gov/mads/rdf/v1#")
MIX = rdflib.Namespace("http://www.jcp.org/jcr/mix/1.0")
MODE = rdflib.Namespace("http://www.modeshape.org/1.0")
NT = rdflib.Namespace("http://www.jcp.org/jcr/nt/1.0")
OWL = rdflib.Namespace("http://www.w3.org/2002/07/owl#")
PREMIS = rdflib.Namespace("http://www.loc.gov/premis/rdf/v1#")
RDF = rdflib.Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
RDFS = rdflib.Namespace("http://www.w3.org/2000/01/rdf-schema#")
SCHEMA = rdflib.Namespace("http://schema.org/")
SV = rdflib.Namespace("http://www.jcp.org/jcr/sv/1.0")
TEST = rdflib.Namespace("info:fedora/test/")
XML = rdflib.Namespace("http://www.w3.org/XML/1998/namespace")
XMLNS = rdflib.Namespace("http://www.w3.org/2000/xmlns/")
XS = rdflib.Namespace("http://www.w3.org/2001/XMLSchema")
XSI = rdflib.Namespace("http://www.w3.org/2001/XMLSchema-instance")


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
    
    def __process_subject__(self, row):
        subject, graph = row
        bf_type = self.__get_specific_type__(subject)
        existing_uri = self.searcher.triplestore.__sameAs__(str(subject))
        if existing_uri:
            subject = rdflib.URIRef(existing_uri)
        fedora_url, new_graph = self.__add_or_get_graph__(
            subject=subject, 
            graph_type=bf_type,
            doc_type=guess_search_doc_type(graph, subject_uri),
            index='bibframe')
        subject_uri = rdflib.URIRef(fedora_url)

    def __clean_up__(self):
        """Internal method performs update on all subjects of the graph, updating
        all internal subject URIs to their corresponding Fedora 4 URIs. """
        for subject, graph in self.subjects:
            pass

         


def main():
    """Main function"""
    pass

if __name__ == '__main__':
    main()
