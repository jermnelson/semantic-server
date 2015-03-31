__author__ = "Jeremy Nelson"

import datetime
import falcon
import rdflib
import sys

from elasticsearch import Elasticsearch
from .. import CONTEXT, INDEXING, RDF, Search
from ..resources import fedora
from .fuseki import Fuseki

def default_graph():
    """Function generates a new rdflib Graph and sets all namespaces as part
    of the graph's context"""
    new_graph = rdflib.Graph()
    for key, value in CONTEXT.items():
        new_graph.namespace_manager.bind(key, value)
    return new_graph

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
            subject_graph.add((subject, p, o))
        # Add a new indexing type
        subject_graph.add((subject, RDF.type, INDEXING.Indexable))
        subject_graphs.append((subject, subject_graph))
    return subject_graphs


class GraphIngester(object):
    """Takes an RDF graph and ingests into Fedora Commons, Fuseki, and 
    Elasticseach. This base class is used by both bibframe.Ingester and
    schema.Ingester child classes in the semantic server"""

    

    def __init__(self, **kwargs):
        self.graph = kwargs.get('graph') 
        self.config = kwargs.get('config')
        self.base_url = kwargs.get('base_url')
        self.searcher = Search(self.config)
        self.subjects = subjects_list(self.graph, self.base_url)     
        self.dedup_predicates = []

    def __add_or_get_graph__(self, subject, graph_type):
        """Helper method takes a subject rdflib.URIRef and graph_type
        to search triple-store and either returns the subject if it 
        already exists or creates a new graph in a Fedora Repository.

        Args:
            subject -- rdflib.URIRef
            graph_type -- Graph type to dedup
        """
        new_graph = default_graph()
        for predicate, object_ in self.graph.predicate_objects(
                                      subject=subject):
            if self.dedup_predicates.count(predicate) > 0:
                exists_url = self.searcher.triplestore.__match__(
                        type=graph_type, 
                        predicate=self.dedup_predicates[
                            self.dedup_predicates.index(predicate)],        
                        object=str(object_))
                if exists_url:
                    return exists_url, rdflib.Graph().parse(exists_url)
            existing_obj_url = self.searcher.triplestore.__sameAs__(object_)
            if existing_obj_url:
                new_graph.add((subject, 
                               predicate, 
                               rdflib.URIRef(existing_obj_url))) 
            else:
                new_graph.add((subject, 
                               predicate, 
                               object_))
        resource = fedora.Resource(self.config)
        resource_url = resource.__create__(rdf=new_graph)
        return resource_url, rdflib.Graph().parse(resource_url)

        
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


    def ingest(self):
        start = datetime.datetime.utcnow()
        print("Started ingesting at {} {}".format(start, len(self.subjects)))
        for i, row in enumerate(self.subjects):
            subject, graph = row
            if not i%10 and i > 0:
                print(".", end="")
            if not i%25:
                print(i, end="")
            #try:
            self.__process_subject__(row)
            #except:
            #    print("Error with {}, subject={}".format(i, subject))
            #    print(sys.exc_info()[0])
            #    break
        end = datetime.datetime.utcnow()
        avg_sec = (end-start).seconds / i
        print("Finished at {}, total subjects {}, Average per min {}".format(
            end,
            i,
            avg_sec / 60.0))
