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
import sys
import urllib.parse
import urllib.request
from flask_fedora_commons import build_prefixes, Repository
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

URL_CHECK_RE = re.compile(
    r'^(?:http|ftp)s?://' # http:// or https://
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' # domain...
    r'localhost|' # localhost...
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|' # ...or ipv4
    r'\[?[A-F0-9]*:[A-F0-9:]+\]?)' # ...or ipv6
    r'(?::\d+)?' # optional port
    r'(?:/?|[/?]\S+)$', re.IGNORECASE)

def create_sparql_insert_row(predicate, object_):
    """Function creates a SPARQL update row based on a predicate and object

    Args:
        predicate(rdflib.Term): Predicate
        object_(rdflib.Term): Object

    Returns:
        string
    """
    statement = "<> "
    if str(predicate).startswith(str(RDF)):
        statement += "rdf:" + predicate.split("#")[-1]
    elif str(predicate).startswith(str(BF)):
        statement += "bf:" + predicate.split("/")[-1]
    elif str(predicate).startswith(str(MADS)):
        statement += "mads:" + predicate.split("#")[-1]
    else:
        statement += "<" + str(predicate) + ">"
    if type(object_) == rdflib.URIRef:
        if URL_CHECK_RE.search(str(object_)):
            statement += " <" + str(object_) + "> "
        else:
            statement += """ "{}" """.format(object_)
    if type(object_) == rdflib.Literal:
        if str(object_).find('"') > -1:
            value = """ '''{}''' """.format(object_)
        else:
            value = """ "{}" """.format(object_)
        statement += value
    if type(object_) == rdflib.BNode:
        statement += """ "{}" """.format(object_)
    statement += ".\n"
    return statement

def dedup(term, elastic_search=Elasticsearch()):
    """Function takes a term and attempts to match it againest three
    subject properties that have been indexed into Elastic search, returns
    the first hit if found.

    Args:
        term(string): A search term or phrase

    Returns:
        string URL of the top-hit for matching the term on a list of
        properties
    """
    if term is None:
        return
    search_result = elastic_search.search(
        index="bibframe",
        body={
            "query": {
                "filtered": {
                    "filter": {
                        "or": [
                            {
                            "term": {
                                "bf:authorizedAccessPoint.raw": term
                            }
                            },
                            {
                            "term": {
                                "mads:authoritativeLabel.raw": term
                            }

                            },
                            {
                            "term": {
                                "bf:label.raw": term
                            }
                            },
                            {
                            "term": {
                                "bf:titleValue.raw": term
                            }
                            }
                    ]
                }
            }
        }
    }
    )
    if search_result.get('hits').get('total') > 0:
        top_hit = search_result['hits']['hits'][0]
        return top_hit['_source']['fcrepo:hasLocation'][0]

def build_sparql(graph):
    #sparql = build_prefixes()
    sparql = "INSERT DATA {\n"
    subjects = list(set(graph.subjects()))
    if type(subjects[0]) == rdflib.BNode or len(subjects) > 1:
        msg = "build_sparql subject cannot be blank or have multiple subjects"
        raise ValueError(msg)
    graph_nt = graph.serialize(format='nt').decode()
    graph_nt = graph_nt.replace(str(subjects[0]), '')
    sparql += graph_nt[:-1] 
    # Add indexing Indexable
    # sparql += "<> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> "
    # sparql += "<http://fedora.info/definitions/v4/indexing#Indexable> .\n"
    return sparql


def ingest_turtle(graph):
    subject = next(graph.subjects(predicate=rdflib.RDF.type))
    raw_turtle = graph.serialize(format='turtle')
    raw_turtle = raw_turtle.decode()
    turtle = raw_turtle.replace("<{}>".format(subject), "<>")
    turtle = turtle[:-3]
    turtle += ";\n    owl:sameAs <{}> .\n\n".format(subject)
    return turtle


def subjects_list(graph):
    """Method takes a BF graph, takes all subjects and creates subject 
    graphs, converting BNodes into fake subject URI

    Args:
       graph(rdf.Graph): BIBFRAME RDF Graph
    
    Returns:
       list: A list of all graphs for each subject in graph
    """
    def __base_url__():
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

            
    def __get_add__(bnode):
        if bnode in bnode_subs:
            return bnode_subs[bnode]
        subject = rdflib.URIRef("{}/{}".format(base_url, bnode))
        bnode_subs[bnode] = subject
        return subject
    bnode_subs, subject_graphs = {}, []
    base_url = __base_url__()
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
        subject_graphs.append(subject_graph)
    return subject_graphs
            
def default_graph():
    """Function generates a new rdflib Graph and sets all namespaces as part
    of the graph's context"""
    new_graph = rdflib.Graph()
    for key, value in CONTEXT.items():
        new_graph.namespace_manager.bind(key, value)
    return new_graph

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


class GraphIngester(object):
    dedup_predicates = [
         BF.authorizedAccessPoint, 
         BF.classificationNumber,
         BF.label, 
         BF.titleValue]
    prefix = """PREFIX bf:<http://bibframe.org/vocab/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>"""
    dedup_sparql = """{}
SELECT ?subject
WHERE {{
    ?subject {{}} "{{}}"^^xsd:string .
    ?subject rdf:type {{}} .
}}""".format(prefix)
    sameAs_sparql = """{}
SELECT DISTINCT ?subject
WHERE {{
  ?subject owl:sameAs <{{}}> .
}}""".format(prefix)
    update_triplestore_sparql = """{}
INSERT INTO {{
   {{}}
}}""".format(prefix)


    def __init__(self, graph, fedora_url=None, fuseki=None, elastic_search=None):
        self.graph = graph 
        self.fedora_url = fedora_url or 'http://localhost:8080/rest'
        self.fuseki_url = fuseki or 'http://localhost:3030'
        self.elastic_search = elastic_search or Elasticsearch()
        self.subjects = subjects_list(self.graph)      

    def __add_or_get_graph__(self, subject):
        new_graph = default_graph()
        bf_type = self.__get_specific_type__(subject)
        for predicate, object_ in self.graph.predicate_objects(
                                      subject=subject):
            if GraphIngester.dedup_predicates.count(predicate) > 0:
                exists_url = self.__dedup_by_predicate__(
                        bf_type, 
                        GraphIngester.dedup_predicates[
                            GraphIngester.dedup_predicates.index(predicate)],        
                        str(object_))
                if exists_url:
                    return rdflib.Graph().parse(exists_url)
            existing_obj_url = self.__get_sameAs__(object_)
            if existing_obj_url:
                new_graph.add((subject, 
                               predicate, 
                               rdflib.URIRef(existing_obj_url))) 
            else:
                new_graph.add((subject, 
                               predicate, 
                               object_))
        fedora_result = requests.post(self.fedora_url,
				      data=ingest_turtle(new_graph),
				      headers={"Content-Type": "text/turtle"})
        if fedora_result.status_code < 400:
            graph_url = fedora_result.text
            return rdflib.Graph().parse(graph_url)    
        else:
            raise falcon.HTTPInternalServerError(
                "Failed to ingest {} into Fedora".format(subject),
                "Error = {}".format(fedora_result.text))
         
             
   
    def __dedup_by_predicate__(bf_type, predicate, obj_value): 
        result = requests.post(
            "/".join([self.fuseki_url, "bf", "query"]),
            data={"query": dedup_sparql.format(predicate, obj_value, bf_type),
                  "output": "json"})
        if result.status_code < 400:
            bindings = result.json().get('results').get('bindings')
            if len(bindings) > 0:
                return bindings[0]['subject']['value']

    def __generate_body__(self, graph):
        self.body = dict()
        bf_json = json.loads(
            graph.serialize(
                format='json-ld',
                context=CONTEXT).decode())
        if '@graph' in bf_json:
            for graph in bf_json.get('@graph'):
                # Index only those graphs that have been created in the
                # repository
                if 'fcrepo:created' in graph:
                    for key, val in graph.items():
                        if key in [
                            'fcrepo:lastModified',
                            'fcrepo:created',
                            'fcrepo:uuid'
                        ]:
                            self.__set_or_expand__(key, val)
                        elif key.startswith('@type'):
                            for name in val:
                                if name.startswith('bf:'):
                                    self.__set_or_expand__('type', name)
                        elif key.startswith('@id'):
                            self.__set_or_expand__('fcrepo:hasLocation', val)
                        elif not key.startswith('fcrepo') and not key.startswith('owl'):
                            self.__set_or_expand__(key, val)
    

    def __get_specific_type__(self, subject):
        for rdf_type in graph.objects(subject=subject, predicate=rdflib.RDF.type):
            if str(rdf_type).startswith("http://bibframe"):
                return "bf:{}".format(str(rdf_type).split("/")[-1])
        # General bf:Resource type as default
        return "bf:Resource"


    def __get_sameAs__(self, url):
        result = requests.post(
            self.fuseki_url, 
            data={"query": sameAs_sparql.format(url), 
                  "output": "json"})
        if result.status_code < 300:
            result_json = result.json()
            if len(result_json.get('results').get('bindings')) > 0:
                return result_json['results']['bindings'][0]['subject']['value']

    def __get_id_or_value__(self, value):
        """Helper function takes a dict with either a value or id and returns
        the dict value

        Args:
	    value(dict)
        Returns:
	    string or None
        """
        if '@value' in value:
            return value.get('@value')
        elif '@id' in value:
            return value.get('@id')
            #! Need to query triplestore?
	    #if uri in self.uris2uuid:
	    #    return self.uris2uuid[uri]
	    #else:
	    #    return uri
        return value

    def __index_subject__(self, subject, graph): 
        self.__generate_body__(graph)
        doc_id = str(graph.value(
                     subject=subject,
                     predicate=FCREPO.uuid))
        doc_type = self.__get_specific_type__(subject).split(":")[-1]
        body = self.generate_body(fcrepo_graph)
        self.elastic_search.index(
            index='bibframe',
            doc_type=doc_type,
            id=doc_id,
            body=self.body)

    def __populate_triplestore__(self, graph):
        update_sparql = GraphIngester.update_triplestore_sparql.format(
            graph.serialize(format='nt'))  
        fuseki_result = requests.post("/".join([self.fuseki_url, 
                                                "bf", 
                                                "update"]),
			      data=update_sparql,
			      headers={"Accept": "text/xml"})
        if fuseki_result.status_code > 399:
            raise falcon.HTTPInternalServerError(
                "Could not create ingest graph into Fuseki.",
		fuseki_result.text)


    
    def __process_subject__(self, subject):
        bf_type = self.__get_specific_type__(subject)
        existing_uri = self.__get_sameAs__(str(subject))
        if existing_uri:
            subject = rdflib.URIRef(existing_uri)
        new_graph = self.__add_or_get_graph__(subject)
        self.__index_subject__(graph)
        self.__populate_triplestore__(graph) 

    def __set_or_expand__(self, key, value):
        """Helper method takes a key and value and either creates a key
        with either a list or appends an existing key-value to the value

        Args:
            key
           value
        """
        if key not in body:
           self.body[key] = []
        if type(value) == list:
            for row in value:
                self.body[key].append(self.__get_id_or_value__(row))
        else:
            self.body[key] = [self.__get_id_or_value__(value),]


    def ingest(self):
        start = datetime.datetime.utcnow()
        print("Started ingesting at {}".format(start))
        for i, subject in enumerate(self.subjects):
            if not i%10 and i > 0:
                print(".", end="")
            if not i%25:
                print(i, end="")
            self.__process_subject__(subject)
        end = datetime.datetime.utcnow()
        avg_sec = (end-start).seconds / i
        print("Finished at {}, total subjects {}, Average per min {}".format(
            end,
            i,
            avg_sec / 60.0))
        
        

class OldGraphIngester(object):
    """Takes a BIBFRAME graph, extracts all subjects and creates an object in
    Fedora 4 for all triples associated with the subject. The Fedora 4 subject
    graph is then indexed into Elastic Search

    To use

    >> ingester = GraphIngester(graph=bf_graph)
    >> ingester.initalize()
    """

    def __init__(self, **kwargs):
        """Initialized a instance of GraphIngester

        Args:
            es(elasticsearch.ElasticSearch): Instance of Elasticsearch
            graph(rdflib.Graph): BIBFRAM RDF Graph
            repository(flask_fedora_commons.Repository): Fedora Commons Repository
            quiet(boolean): If False, prints status of ingestion
            debug(boolean): Adds additional information for debugging purposes

        """
        self.bf2uris = {}
        self.debug = kwargs.get('debug', False)
        self.uris2uuid = {}
        self.elastic_search = kwargs.get('elastic_search', Elasticsearch())
        if not self.elastic_search.indices.exists('bibframe'):
            helper_directory = os.path.dirname(__file__)
            base_directory = helper_directory.split(
                "{0}catalog{0}helpers".format(os.path.sep))[0]
            with open(
                os.path.join(
                    base_directory,
                    "search{0}config{0}bibframe-map.json".format(
                    os.path.sep))) as raw_json:
                        bf_map = json.load(raw_json)
            self.elastic_search.indices.create(index='bibframe', body=bf_map)

        self.graph = kwargs.get('graph', default_graph())
        self.repository = kwargs.get('repository', Repository())
        self.quiet = kwargs.get('quiet', False)

    def init_subject(self, subject):
        """Method initializes a subject, serializes JSON-LD of Fedora container
        and then a simplified indexed into the Elastic Search instance.

        Args:
            subject(rdflib.Term): Subject

	   Returns:
            fedora_url
        """
        if str(subject) in self.bf2uris:
            return
        existing_url =  self.exists(subject)
        if existing_url:
            if not str(subject) in self.bf2uris:
                self.bf2uris[str(subject)] = existing_url
            return
        raw_turtle = """PREFIX bf: <http://bibframe.org/vocab/>
 PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
 PREFIX mads: <http://www.loc.gov/mads/rdf/v1#>\n"""
        for predicate, _object in self.graph.predicate_objects(subject=subject):
            if type(_object) == rdflib.Literal:
                raw_turtle += create_sparql_insert_row(predicate, _object)
            if predicate == rdflib.RDF.type:
                raw_turtle += create_sparql_insert_row(predicate, _object)

        new_request = urllib.request.Request(
            "/".join([self.repository.base_url, 'rest']),
            data=raw_turtle.encode(),
            method="POST",
            headers={"Content-Type": "text/turtle"})
        try:
            fedora_url = urllib.request.urlopen(new_request).read().decode()
        except urllib.error.HTTPError as http_error:
            error_comment = "Failed to add {}, Error={}\nTurtle=\n{}".format(
                subject,
                http_error,
                raw_turtle)
            print(error_comment)
            fedora_url = self.repository.create()
            self.repository.insert(fedora_url, "owl:sameAs", str(subject))
            ##self.repository.insert(fedora_url, "rdfs:comment", error_comment)
        self.bf2uris[str(subject)] = fedora_url
        self.index(rdflib.URIRef(fedora_url))
        return fedora_url

    def generate_body(self, graph):
        """Function takes a Fedora URI, filters the Fedora graph and returns a dict
        for indexing into Elastic search

        Args:
            graph(rdflib.Graph): Fedora Graph

        Returns:
            dict: Dictionary of values filtered for Elastic Search indexing
        """
        def get_id_or_value(value):
            """Helper function takes a dict with either a value or id and returns
            the dict value

            Args:
                value(dict)
            Returns:
                string or None
            """
            if '@value' in value:
                return value.get('@value')
            elif '@id' in value:
                uri = value.get('@id')
                if uri in self.uris2uuid:
                    return self.uris2uuid[uri]
                else:
                    return uri
            return value
        def set_or_expand(key, value):
            """Helper function takes a key and value and either creates a key
            with either a list or appends an existing key-value to the value

            Args:
                key
                value
            """
            if key not in body:
                body[key] = []
            if type(value) == list:
                for row in value:
                    body[key].append(get_id_or_value(row))
            else:
                body[key] = [get_id_or_value(value),]
        body = dict()
        bf_json = json.loads(
            graph.serialize(
                format='json-ld',
                context=CONTEXT).decode())
        if '@graph' in bf_json:
            for graph in bf_json.get('@graph'):
                # Index only those graphs that have been created in the
                # repository
                if 'fcrepo:created' in graph:
                    for key, val in graph.items():
                        if key in [
                            'fcrepo:lastModified',
                            'fcrepo:created',
                            'fcrepo:uuid'
                        ]:
                            set_or_expand(key, val)
                        elif key.startswith('@type'):
                            for name in val:
                                if name.startswith('bf:'):
                                    set_or_expand('type', name)
                        elif key.startswith('@id'):
                            set_or_expand('fcrepo:hasLocation', val)
                        elif not key.startswith('fcrepo') and not key.startswith('owl'):
                            set_or_expand(key, val)
        return body




    def exists(self, subject):
        """Method takes a subject, queries Elasticsearch index,
        if present, adds result to bf2uris, always returns boolean

        Args:
            subject(rdflib.Term): Subject, can be literal, BNode, or URI

        Returns:
            boolean
        """
        doc_type = guess_search_doc_type(self.graph, subject)
        # This should be accomplished in a single Elasticseach query
        # instead of potentially five separate queries
        for predicate in [
            BF.authorizedAccessPoint,
            BF.label,
            MADS.authoritativeLabel,
            BF.titleValue]:
            objects = self.graph.objects(
                subject=subject,
                predicate=predicate)
            for object_value in objects:
                result = dedup(str(object_value), self.elastic_search)
                if result:
                    return result


    def index(self, fcrepo_uri):
        """Method takes a Fedora Object URIRef, generates JSON-LD
        representation, and then ingests into an Elasticsearch
        instance

        Args:
            fcrepo_uri(rdflib.URIRef): Fedora URI Ref for a BIBFRAME subject

        """
        fcrepo_graph = default_graph().parse(str(fcrepo_uri))
        doc_id = str(fcrepo_graph.value(
                    subject=fcrepo_uri,
                    predicate=FCREPO.uuid))
        if not str(fcrepo_uri) in self.uris2uuid:
            self.uris2uuid[str(fcrepo_uri)] = doc_id
        doc_type = guess_search_doc_type(fcrepo_graph, fcrepo_uri)
        body = self.generate_body(fcrepo_graph)
        self.elastic_search.index(
            index='bibframe',
            doc_type=doc_type,
            id=doc_id,
            body=body)

    def ingest(self):
        """Method ingests a BIBFRAME graph into Fedora 4 and Elastic search"""
        start = datetime.datetime.utcnow()
        if self.quiet is False:
            print("Started ingestion at {}".format(start.isoformat()))
        subjects = set([subject for subject in self.graph.subjects()])
        if self.quiet is False:
            print("Initializing all subjects")
        for i, subject in enumerate(subjects):
            if not i%10 and i > 0:
                if self.quiet is False:
                    print(".", end="")
            if not i%100:
                if self.quiet is False:
                    print(i, end="")
            self.init_subject(subject)
        finished_init = datetime.datetime.utcnow()
        if self.quiet is False:
            print("Finished initializing {} subjects at {}, time={}".format(
                i,
                finished_init,
                (finished_init-start).seconds / 60.0))
        for i, subject_uri in enumerate(subjects):
            if not i%10 and i > 0:
                if self.quiet is False:
                    print(".", end="")
            if not i%100:
                if self.quiet is False:
                    print(i, end="")
            self.process_subject(subject_uri)
        end = datetime.datetime.utcnow()
        if self.quiet is False:
            print("Finished ingesting at {}, total time={} minutes for {} subjects".format(
                end.isoformat(),
                (end-start).seconds / 60.0,
                i))

    def process_subject(self, subject):
        """Method takes a subject URI and iterates through the subject's
        predicates and objects, saving them to a the subject's Fedora graph.
        Blank nodes are expanded and saved as properties to the subject
        graph as well. Finally, the graph is serialized as JSON-LD and updated
        in the Elastic Search index.

        Args:
            subject(rdflib.URIRef): Subject URI
        """
        fedora_url = self.bf2uris[str(subject)]
        sparql = build_prefixes(self.repository.namespaces)
        sparql += "\nINSERT DATA {\n"
        if self.debug:
            sparql += create_sparql_insert_row(
                OWL.sameAs,
                subject
            )
        for predicate, _object in self.graph.predicate_objects(subject=subject):
            if str(_object) in self.bf2uris:
                object_url = self.bf2uris[str(_object)]
                sparql += create_sparql_insert_row(
                    predicate,
                    rdflib.URIRef(object_url)
                )
            elif _object != rdflib.Literal:
                sparql += create_sparql_insert_row(
                    predicate,
                    _object)
        sparql += "\n}"
        update_fedora_request = urllib.request.Request(
            fedora_url,
            method='PATCH',
            data=sparql.encode(),
            headers={"Content-type": "application/sparql-update"})
        try:
            result = urllib.request.urlopen(update_fedora_request)
            self.index(rdflib.URIRef(fedora_url))
            return fedora_url
        except:
            print("Could NOT process subject {} Error={}".format(
                subject,
                sys.exc_info()[0]))
            print(fedora_url)
            print(sparql)


def main():
    """Main function"""
    pass

if __name__ == '__main__':
    main()
