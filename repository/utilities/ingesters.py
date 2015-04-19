__author__ = "Jeremy Nelson"

import datetime
import falcon
import logging
import rdflib
import sys
import urllib.parse

from elasticsearch import Elasticsearch
from .. import CONTEXT, INDEXING, RDF, Search, default_graph
from ..resources import fedora
from ..resources.fuseki import TripleStore
from .namespaces import *


def valid_uri(uri):
    """function takes a rdflib.URIRef and checks if it is valid for 
    serialization, quotes path if invalid.

    Args:
        uri -- rdflib.URIRef
    """
    try:
        rdflib.URIRef(str(uri))
    except:
        url = urllib.parse.urlparse(str(uri))
        new_url = url.geturl().replace(url.path, urllib.parse.quote(url.path))
        uri = rdflib.URIRef(new_url)
    return uri


def subjects_list(graph, base_url):
    """Method from a RDF graph, takes all subjects and creates separate 
    subject graphs, converting BNodes into fake subject URIs

    Args:
       graph(rdf.Graph): BIBFRAME RDF Graph
       base_url: URL pattern    
    Returns:
       list: A list of all graphs for each subject in graph
    """
    def __get_add__(bnode):
        if bnode in bnode_subs:
            return bnode_subs[bnode]
        subject = rdflib.URIRef("{}/{}".format(base_url, bnode))
        bnode_subs[bnode] = subject
        return subject
    bnode_subs, subject_graphs = {}, []
    subjects = list(set(graph.subjects()))
    for s in subjects:
        subject_graph = default_graph()
        if type(s) == rdflib.BNode:
            subject = __get_add__(s)
        else:
            subject = s
        # Now add all subject's triples to subject graph
        for p,o in graph.predicate_objects(s):
            if type(o) == rdflib.BNode:
                o = __get_add__(o)
            if type(o) == rdflib.URIRef:
                o = valid_uri(o)
            subject_graph.add((subject, p, o))
        # Add a new indexing type
        subject_graph.add((subject, RDF.type, INDEXING.Indexable))
        subject_graphs.append([subject, subject_graph, False])
    return subject_graphs


class GraphIngester(object):
    """Takes an RDF graph and ingests into Fedora Commons, Fuseki, and 
    Elasticseach. This base class is used by both bibframe.Ingester and
    schema.Ingester child classes in the semantic server"""


    def __init__(self, **kwargs):
        self.graph = kwargs.get('graph') 
        self.config = kwargs.get('config')
        self.base_url = kwargs.get('base_url')
        self.searcher = kwargs.get('search', Search(self.config))
        self.subjects = subjects_list(self.graph, self.base_url)     
        self.dedup_predicates = []


        
    def __add_or_get_graph__(self, **kwargs):
        """Helper method takes a subject rdflib.URIRef and graph_type
        to search triple-store and either returns the subject if it 
        already exists or creates a new graph in a Fedora Repository.

        Keyword args:
            subject -- rdflib.URIRef
            graph -- Graph to ingest, default is instance's original graph
            graph_type -- Graph type to dedup
            index -- Elastic search index, defaults to None    
            doc_type -- Elastic search doc type for graph, defaults to None
        """ 
        doc_type=kwargs.get('doc_type', None)
        index=kwargs.get('index', None)
        graph = kwargs.get('graph', self.graph)
        graph_type = kwargs.get('graph_type')
        new_graph = default_graph()
        subject = kwargs.get('subject') 
        for predicate, object_ in graph.predicate_objects(
                                      subject=subject):
            if self.dedup_predicates.count(predicate) > 0:
                exists_url = self.searcher.triplestore.__match__(
                        type=graph_type, 
                        predicate=self.dedup_predicates[
                            self.dedup_predicates.index(predicate)],        
                        object=str(object_))
                if exists_url:
                    return exists_url, rdflib.Graph().parse(exists_url)
            if type(object_) == rdflib.URIRef:
                existing_obj_url = self.searcher.triplestore.__sameAs__(object_)
                if existing_obj_url:
                    new_graph.add((subject, 
                                   predicate, 
                                   rdflib.URIRef(existing_obj_url))) 
                    continue
            new_graph.add((subject, 
                           predicate, 
                           object_))
        resource = fedora.Resource(self.config, self.searcher)
        resource_url = resource.__create__(
            rdf=new_graph, 
            subject=subject, 
            doc_type=doc_type,
            index=index
        )
        return resource_url, default_graph().parse(resource_url)


    def __clean_up__(self):
        """Internal method performs update on all subjects of the graph, updating
        all internal subject URIs to their corresponding Fedora 4 URIs. The last
        subject graphs processed should have correct references, earlier ones may
        not. This method may be overridden by child classes"""
        for subject, graph, ingested in self.subjects:
            if ingested is False:
                continue
            local_url = str(subject)
            fedora_url = self.searcher.triplestore.__sameAs__(local_url)
            for row in self.searcher.triplestore.__get_fedora_local__(local_url):
                predicate = row['predicate']['value']
                subject = row['subject']['value']
                repository_result = fedora.replace_property(
                    subject, predicate, local_url, fedora_url)
                if not self.searcher.triplestore.__replace_object__(
                    subject,
                    rdflib.URIRef(predicate),
                    rdflib.URIRef(local_url),
                    rdflib.URIRef(fedora_url)):
                    print("Could not update triplestore with fedora urls")
                

    def __get_types__(self, subject, startstr, prefix):
        types = []
        for rdf_type in self.graph.objects(
            subject=subject,
            predicate=rdflib.RDF.type):
            if str(rdf_type).startswith(startstr):
                name = str(rdf_type).split("/")[-1]
                if name.startswith("#"):
                    name = name[1:]
                types.append("{}:{}".format(prefix, name))
        return types


    def __process_subject__(self, row):
        """Helper method should be overridden by implementing 
        child classes.

        Args:
            row -- Tuple of rdflib.URIRef subject and it's corresponding 
                   RDF graph
        """
        pass


    def ingest(self, quiet=True):
        start = datetime.datetime.utcnow()
        if not quiet:
            print("Started ingesting at {} {}".format(start, len(self.subjects)))
        for i, row in enumerate(self.subjects):
            subject, graph = row[0], row[1]
            if not i%10 and i > 0 and not quiet:

                print(".", end="")
            if not i%25 and not quiet:
                print(i, end="")
            try:
               self.__process_subject__(row)
               row[2] = True

            except:
                logging.error("Error with {}, subject={}\n\t{}".format(
                    i, 
                    subject,
                    sys.exc_info()[0:2]))
                break
        self.__clean_up__()
        end = datetime.datetime.utcnow()
        if not i:
            i = 1
        avg_sec = (end-start).seconds / i
        if not quiet:
            print("Finished at {}, total subjects {}, Average per min {}".format(
            end,
            i,
            avg_sec / 60.0))
