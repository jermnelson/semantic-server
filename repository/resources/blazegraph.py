__author__ = "Jeremy Nelson"

import falcon
import requests
from ..utilities.sparql_templates import *

class TripleStore(object):

    def __init__(self, config={}):
        self.config = config
        self.url = config.get("BLAZEGRAPH").get("url")


    def __load__(self, rdf):
        """Internal Method loads a RDF graph into Fuseki

        Args:
            rdf -- rdflib.Graph
        Raises:
            falcon.HTTPInternalServerError
        """
        blaze_result = requests.post(self.url,
            data=rdf.serialize(),
            headers={"Content-Type": "application/rdf+xml"})
        if blaze_result.status_code > 399:
            raise falcon.HTTPInternalServerError(
                "Failed to load RDF into {}".format(self.url),
                "Error:\n{}\nRDF:\n{}".format(
                    fuseki_result.text,
                    rdf))

    def __match__(self, **kwargs):    
        predicate = kwargs.get('predicate')
        object_ = kwargs.get('object')
        type_ = kwargs.get('type')
          
        result = requests.post(
            self.url,
            data={"query": DEDUP_SPARQL.format(
                               predicate, 
                               object_, 
                               type_),
                  "format": "json"})
       
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
        if URL_CHECK_RE.search(object_):
            insert_str += ' <{}> '.format(object_)
        elif PREFIX_CHECK_RE.search(object_):
            insert_str += ' {} '.format(object_)
        else:
            insert_str += ' "{}" '.format(object_)
        sparql = UPDATE_TRIPLESTORE_SPARQL.format(insert_str)
        result = requests.post(
            self.url,
            data={"update": sparql, "output": "json"})
        if result.status_code < 400:
            raise falcon.HTTPInternalServerError(
                "Failed to update triple",
                "Failed to update {} {} {} to Fuseki.\nError={}".format(
                    subject,
                    predicate,
                    object_,
                    result.text)) 
       

