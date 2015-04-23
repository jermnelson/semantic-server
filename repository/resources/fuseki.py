__author__ = "Jeremy Nelson"

import falcon
import json
import rdflib
import re
import requests
from ..utilities.namespaces import *

PREFIX = """PREFIX fedora: <{}>
PREFIX owl: <{}>
PREFIX rdf: <{}>
PREFIX xsd: <{}>""".format(FCREPO, OWL, RDF, XSD)


DEDUP_SPARQL = """{}
SELECT ?subject
WHERE {{{{
    ?subject <{{}}> "{{}}"^^xsd:string .
    ?subject rdf:type <{{}}> .
}}}}""".format(PREFIX)

GET_ID_SPARQL = """{}
SELECT ?uuid
WHERE {{{{
 <{{}}> fedora:uuid ?uuid .
}}}}""".format(PREFIX)

LOCAL_SUBJECT_PREDICATES_SPARQL = """{}
SELECT DISTINCT *
WHERE {{{{
  ?subject ?predicate <{{0}}> .
  FILTER NOT EXISTS {{{{ ?subject owl:sameAs <{{0}}> }}}}

}}}}""".format(PREFIX)

REPLACE_OBJECT_SPARQL = """{}
DELETE {{{{
    <{{0}}> {{1}} {{2}} .
}}}}
INSERT {{{{
    <{{0}}> {{1}} {{3}} 
}}}}
WHERE {{{{
}}}}""".format(PREFIX)


SAME_AS_SPARQL = """{}
SELECT DISTINCT ?subject
WHERE {{{{
  ?subject owl:sameAs {{}} .
}}}}""".format(PREFIX)


UPDATE_TRIPLESTORE_SPARQL = """{}
INSERT DATA {{{{
   {{}}
}}}}""".format(PREFIX)

PREFIX_CHECK_RE = re.compile(r'\w+[:][a-zA-Z]')


URL_CHECK_RE = re.compile(
    r'^(?:http|ftp)s?://' # http:// or https://
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' # domain...
    r'localhost|' # localhost...
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|' # ...or ipv4
    r'\[?[A-F0-9]*:[A-F0-9:]+\]?)' # ...or ipv6
    r'(?::\d+)?' # optional port
    r'(?:/?|[/?]\S+)$', re.IGNORECASE)


class TripleStore(object):
    """Implements a Fuseki Triplestore REST API and Management Functions 
    for the semantic server.

    >> import fuseki
    >> triplestore = fuseki.TripleStore(config)
    
    """

    def __init__(self, config={}):
        """Initialize a Fuseki TripleStore class 

        Args:
            config -- dictionary or loaded configparser
        """
        if not "FUSEKI" in config:
            url = "http://localhost:3030"
            datastore = 'ds'
        else: 
            url = "http://{}:{}".format(
                config["FUSEKI"]["host"],
                config["FUSEKI"]["port"])
            datastore = config["FUSEKI"]["datastore"]
        self.update_url = "/".join([url, datastore, "update"])
        self.query_url = "/".join([url, datastore, "query"])


    def __get_id__(self, fedora_url):
        """Internal method takes a Fedora URL and returns the uuid associated
        with the Fedora Resource

        Args:
            fedora_url -- Fedora URL
        """
        result = requests.post(
            self.query_url,
            data={"query":  GET_ID_SPARQL.format(fedora_url),
                  "output": "json"})
        if result.status_code < 400:
            return result.json().get('results').get('bindings')
        else:
            raise falcon.HTTPInternalServerError(
                "Failed to retrieve uuid",
                "Failed to retreive fedora:uuid for {}. Error:\n{}".format(
                    fedora_url,
                    result.content))

        

    def __get_fedora_local__(self, local_url):
        """Internal method takes a local url and returns all of its 
        subject, object references in the triple-store

        Args:
            local_url -- Local URL 
        Returns:
            List of dicts with Fedora URL and predicate
        """
        result = requests.post(
            self.query_url,
            data={"query": LOCAL_SUBJECT_PREDICATES_SPARQL.format(local_url),
                  "output": "json"})
        if result.status_code < 400:
            return result.json().get('results').get('bindings')
        else:
            raise falcon.HTTPInternalServerError(
                "Failed to return all Fedora URLs",
                "Local URL={}\nError: {}".format(
                    local_url,
                    result.text))
                     
    def __get_subject__(self, **kwargs):
        """Internal method searches for and returns a unique match 
        based on what type of search being performed.

        Keyword args:
           uuid -- Returns subject that matches by Fedora uuid
        """
        sparql = None
        if 'uuid' in kwargs:
            sparql = """{}
PREFIX fedora: <http://fedora.info/definitions/v4/repository#> 
SELECT DISTINCT ?subject
WHERE {{{{
    ?subject fedora:uuid '''{}'''^^xsd:string .
}}}}""".format(PREFIX, kwargs.get('uuid'))
        if sparql is None:
            raise falcon.falcon.HTTPNotAcceptable(
                "Failed to get subject",
                "Missing SPARQL") 
        result = requests.post(
            self.query_url,
            data={"query":  sparql,
                  "output": "json"})
        if result.status_code < 400:
            return result.json().get('results').get('bindings')
        else:
            description = "Subject keys and values:"
            for key, value in kwargs.items():
                description += "\n{}={}".format(key, value)
            raise falcon.HTTPInternalServerError(
                "Failed to get subject",
                description)
        

    def __match__(self, **kwargs):
        """Internal method attempts to match an existing subject
        in the triple-store based on the subject's type and a 
        string or hash value

	Keyword arguments:
            predicate -- rdflib Predicate
            object -- rdflib Object
            type -- RDF type to restrict query on
        """
        predicate = kwargs.get('predicate')
        object_ = kwargs.get('object')
        type_ = kwargs.get('type')
          
        result = requests.post(
            self.query_url,
            data={"query": DEDUP_SPARQL.format(
                               predicate, 
                               object_, 
                               type_),
                  "output": "json"})
        if result.status_code < 400:
            bindings = result.json().get('results').get('bindings')
            if len(bindings) > 0:
                return bindings[0]['subject']['value']
        else:
            raise falcon.HTTPInternalServerError(
                "Failed to match query in Fuseki",
                "Predicate={} Object={} Type={}\nError:\n{}".format(
                    predicate,
                    object_,
                    type_,
                    result.text))

#    def __replace_all__(self, **kwargs):
#        """Internal Method replaces all occurrences in t

    def __load__(self, rdf):
        """Internal Method loads a RDF graph into Fuseki

        Args:
            rdf -- rdflib.Graph
        Raises:
            falcon.HTTPInternalServerError
        """
        fuseki_result = requests.post(self.update_url,
            data={"update": UPDATE_TRIPLESTORE_SPARQL.format(
                             rdf.serialize(format='nt').decode())})
        if fuseki_result.status_code > 399:
            raise falcon.HTTPInternalServerError(
                "Failed to load RDF into {}".format(self.update_url),
                "Error:\n{}\nRDF:\n{}".format(
                    fuseki_result.text,
                    rdf))

    def __replace_object__(self, subject, predicate, old_object, new_object):
        """Internal method attempts to replace an existing triple's object with
        a new object

        Args:
           subject -- rdflib.URIRef
           predicate -- rdflib.URIRef
           old_object -- rdflib.URIRef or rdflib.Literal
           new_object -- rdflib.URIRef or rdflib.Literal
        """
        def get_string_rep(object_):
            if type(object_) == rdflib.URIRef:
                return '<{}>'.format(object_)
            elif type(old_object) == rdflib.Literal:
                return '"{}"'.format(object_)
        sparql = REPLACE_OBJECT_SPARQL.format(
            subject,
            get_string_rep(predicate),
            get_string_rep(old_object),
            get_string_rep(new_object))
        result = requests.post(
            self.update_url,
            data={"update": sparql, "output": "json"})
        if result.status_code < 400:
            return True
        else:
            raise falcon.HTTPInternalServerError(
                "Failed to replace triple's object",
                "Error:\n{}\nSubject:{}\tPredicate:{}\tOld Obj:{}\tNew Obj:{}".format(
                    result.text,
                    subject,
                    predicate,
                    old_object,
                    new_object))


    def __sameAs__(self, url):
        """Internal method takes a url and attempts to retrieve any existing
        subjects with the equivalent owl:sameAs 

        Args:
            url -- Subject URL
        """
        if URL_CHECK_RE.search(url):
            object_str = "<{}>".format(url)
        else: # Test as a string literal
            object_str = """"{}"^^xsd:string""".format(url)        
        result = requests.post(
            self.query_url, 
            data={"query": SAME_AS_SPARQL.format(object_str), 
                  "output": "json"})
        if result.status_code < 400:
            result_json = result.json()
            if len(result_json.get('results').get('bindings')) > 0:
                return result_json['results']['bindings'][0]['subject']['value']
        else:
             raise falcon.HTTPInternalServerError(
                "Failed to run sameAs query in Fuseki",
                "URL={}\nError {}:\n{}".format(url, result.status_code, result.text))

    def __update_triple__(self, subject, predicate, object_):
        """Internal method updates a subject, predicate, and object in Fuseki

        Args:
            subject -- Subject URL
            predicate -- Predicate URL or use prefix notation
            object_ -- Literal, URL, or URL with prefix
        """
        insert_str = '<{}>'.format(subject)
        if PREFIX_CHECK_RE.search(predicate):
            insert_str += " {} ".format(predicate)
        elif URL_CHECK_RE.search(predicate):
            insert_str += " <{}> ".format(predicate)
        else:
            insert_str += ' "{}" '.format(predicate)
        if URL_CHECK_RE.seach(object_):
            insert_str += ' <{}> '.format(object_)
        elif PREFIX_CHECK_RE.search(object_):
            insert_str += ' {} '.format(object_)
        else:
            insert_str += ' "{}" '.format(object_)
        sparql = UPDATE_TRIPLESTORE_SPARQL.format(insert_str)
        result = requests.post(
            self.update_url,
            data={"sparql": sparql, "output": "json"})
        if result.status_code < 400:
            raise falcon.HTTPInternalServerError(
                "Failed to update triple",
                "Failed to update {} {} {} to Fuseki.\nError={}".format(
                    subject,
                    predicate,
                    object_,
                    result.text))

    def on_get(self, req, resp):
        """GET method returns information related to the Fuseki Instance

        Args:
           req -- HTTP Request
           resp -- HTTP Response
        """ 
        resp.status = falcon.HTTP_200
        resp.body = json.dumps({"message": "Fuseki is active",
                                "query_url": self.query_url})

    def on_post(self, req, resp):
        
        sparql = req.get_param('sparql') or None
        print("IN FUSEKI POST sparql={} url={}".format(sparql, self.query_url))
        if sparql is None:
            raise falcon.HTTPMissingParam('sparql')
        result = requests.post(
            self.query_url,
            data={"query": sparql, "output": "json"})
        if result.status_code > 399:
            raise falcon.HTTPInternalServerError(
                "Fuseki Server Error",
                result.text)
        resp.status = falcon.HTTP_200
        print("Result {}".format(result.json()))
        resp.body = json.dumps(result.json())

    def on_patch(self, req, resp):
        """PATCH method takes updates Fuseki with SPARQL as a request parameter

         Args:
            req -- HTTP Request
            resp -- HTTP Response
        """       
        resp.status = falcon.HTTP_200         

    def on_put(self, req, resp):
        rdf = req.get_param('rdf') or None
        if rdf:
            self.__load__(self.rdf)
            msg = "Successfully loaded RDF into Fuseki"
        else:
            msg = "No RDF to load into Fuseki"
        resp.status = falcon.HTTP_201
        resp.body = json.dumps({"message": msg})

