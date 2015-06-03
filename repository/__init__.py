"""Module wraps Fedora Commons repository, Elasticsearch, and Fuseki
into single API for use in such projects as the BIBFRAME Catalog, TIGER Catalog,
Islandora eBadges, Schema.org Editor, and Django BFE projects.
"""
__author__ = "Jeremy Nelson"

import falcon
import json
import rdflib
import re
import urllib.request

from elasticsearch import Elasticsearch
from .resources.fuseki import TripleStore
from .utilities.namespaces import *

CONTEXT = {
    "authz": str(AUTHZ),
    "bf": str(BF),
    "dc": str(DC),
    "fedora": str(FEDORA),
    "fedoraconfig": str(FEDORACONFIG),
    "fedorarelsext": str(FEDORARELSEXT),
    "foaf": str(FOAF),
    "image": str(IMAGE),
    "iana": str(IANA),
    "indexing": "http://fedora.info/definitions/v4/indexing#",
    "ldp": str(LDP),
    "mads": str(MADS),
    "mix": str(MIX),
    "mode": "http://www.modeshape.org/1.0",
    "owl": "http://www.w3.org/2002/07/owl#",
    "pto": str(PTO),
    "nt": "http://www.jcp.org/jcr/nt/1.0",
    "premis": "http://www.loc.gov/premis/rdf/v1#",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "schema": str(SCHEMA),
    "sv": "http://www.jcp.org/jcr/sv/1.0",
    "test": "info:fedora/test/",
    "xml": "http://www.w3.org/XML/1998/namespace",
    "xmlns": "http://www.w3.org/2000/xmlns/",
    "xs": "http://www.w3.org/2001/XMLSchema",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance"}

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

def default_graph():
    """Function generates a new rdflib Graph and sets all namespaces as part
    of the graph's context"""
    new_graph = rdflib.Graph()
    for key, value in CONTEXT.items():
        new_graph.namespace_manager.bind(key, value)
    return new_graph

def generate_prefix():
    prefix = ''
    for key, value in CONTEXT.items():
        prefix += 'PREFIX {}:<{}>\n'.format(key, value)
    return prefix 

def ingest_resource(req, resp, resource):
    """Decorator function for ingesting a Resource into Elastic Search
    and Fuseki

    Arguements:
        req -- Request
        resp -- Response
        resource -- Parameters
    """
    body = json.loads(resp.body)
    fcrepo_uri = rdflib.URIRef(body['uri'])
    graph = rdflib.Graph().parse(body['uri'])
    doc_id = str(graph.value(
        subject=fcrepo_uri,
        predicate=FEDORA.uuid))

    TripleStore(resource.config).__load__(fuseki_sparql)

def ingest_turtle(graph):
    subjects = [s for s in set(graph.subjects())]
    subject = subjects[0]
    try: 
        raw_turtle = graph.serialize(format='turtle').decode()
        turtle = raw_turtle.replace("<{}>".format(subject), "<>")
        turtle = turtle[:-3]
        turtle += ";\n    owl:sameAs <{}> .\n\n".format(subject)
    except:
        turtle = ""
        for predicate, object_ in graph.predicate_objects():
            turtle += create_sparql_insert_row(predicate, object_)
    return turtle

class Info(object):
    """Basic information about available repository services"""

    def __init__(self, config):
        self.config = config

    def on_get(self,  req, resp):
        resp.status = falcon.HTTP_200
        resp.body = json.dumps(
            {"services": str(self.config)}
        )

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

def generate_body(graph):
    """Function takes a Fedora URI, filters the Fedora graph and returns a dict
    for indexing into Elastic search

    Args:
        graph(rdflib.Graph): Fedora Graph

    Returns:
        dict: Dictionary of values filtered for Elastic Search indexing
    """
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

class Search(object):
    """Search Repository"""

    def __init__(self, config):
        if 'ELASTICSEARCH' in config:
            options = {"host": config["ELASTICSEARCH"]["host"],
                       "port": config["ELASTICSEARCH"]["port"]}
            if 'url_prefix' in config["ELASTICSEARCH"]:
                options['url_prefix'] = config["ELASTICSEARCH"]['url_prefix']
            self.search_index = Elasticsearch(options)
        self.triplestore = TripleStore(config)
        self.body = None

    def __get_id_or_value__(self, value):
        """Helper function takes a dict with either a value or id and returns
        the dict value

        Args:
	    value(dict)
        Returns:
	    string or None
        """
        if [str, float, int, bool].count(type(value)) > 0:
            return value 
        elif '@value' in value:
            return value.get('@value')
        elif '@id' in value:
            result = self.triplestore.__get_id__(value.get('@id'))
            if len(result) > 0:
                return result[0]['uuid']['value']
            return value.get('@id')
        return value

    def __generate_body__(self, graph, prefix=None):
        """Internal method generates the body for indexing into Elastic search
        based on the JSON-LD serializations of the Fedora Commons Resource graph.

        Args:
            graph -- rdflib.Graph of Resource
            prefix -- Prefix filter, will only index if object starts with a prefix,
                      default is None to index everything.
        """
        self.body = dict()
        graph_json = json.loads(
            graph.serialize(
                format='json-ld',
                context=CONTEXT).decode())
        if '@graph' in graph_json:
            for graph in graph_json.get('@graph'):
                # Index only those graphs that have been created in the
                # repository
                if 'fedora:created' in graph:
                    for key, val in graph.items():
                        if key in [
                            'fedora:lastModified',
                            'fedora:created',
                            'fedora:uuid'
                        ]:
                            self.__set_or_expand__(key, val)
                        elif key.startswith('@type'):
                            for name in val:
                                #! prefix should be a list 
                                if prefix:
                                    if name.startswith(prefix):
                                        self.__set_or_expand__('type', name)
                                else:
                                    self.__set_or_expand__('type', name)
                        elif key.startswith('@id'):
                            self.__set_or_expand__('fedora:hasLocation', val)
                        elif not key.startswith('fedora') and not key.startswith('owl'):
                            self.__set_or_expand__(key, val) 


    def __index__(self, subject, graph, doc_type, index, prefix=None): 
        self.__generate_body__(graph, prefix)
        doc_id = str(graph.value(
                     subject=subject,
                     predicate=FEDORA.uuid))
        self.__generate_suggestion__(subject, graph, doc_id)
        self.search_index.index(
            index=index,
            doc_type=doc_type,
            id=doc_id,
            body=self.body)

    def __set_or_expand__(self, key, value):
        """Helper method takes a key and value and either creates a key
        with either a list or appends an existing key-value to the value

        Args:
            key
            value
        """
        if key not in self.body:
           self.body[key] = []
        if type(value) == list:
            for row in value:
                self.body[key].append(self.__get_id_or_value__(row))
        else:
            self.body[key].append(self.__get_id_or_value__(value))

    def __update__(self, **kwargs):
        """Helper method updates a stored document in Elastic Search and Fuseki. 
        Method must have doc_id 

        Keyword args:
            doc_id -- Elastic search document ID
            field -- Field name to update index, raises exception if None
            value -- Field value to update index, raises exception if None
        """
        doc_id, doc_type, index = kwargs.get('doc_id'), None, None
        if not doc_id:
            raise falcon.HTTPMissingParam("doc_id")
        field = kwargs.get('field')
        if not field:
            raise falcon.HTTPMissingParam("field")
        value = kwargs.get('value')
        if not value:
            raise falcon.HTTPMissingParam("field")
        for row in self.search_index.indices.stats()['indices'].keys():
            # Doc id should be unique across all indices 
            if self.search_index.exists(index=row, id=doc_id): 
                result = self.search_index.get(index=row, id=doc_id)
                doc_type = result['_type']
                index=row
                break
        if doc_type is None or index is None:
            raise falcon.HTTPNotFound()                 
        self.search_index.update(
            index=index,
            doc_type=doc_type,
            id=doc_id,
            body={"doc": {
                field: self.__get_id_or_value__(value)
            }})
        result = self.triplestore.__get_subject__(uuid=doc_id)
        if len(result) == 1:
            self.triplestore.__update_triple__(
                result[0]['subject']['value'], 
                field, 
                value)         
            

    def on_get(self, req, resp):
        """Method takes a a phrase, returns the expanded result.

        Args:
            req -- Request
            resp -- Response
        """
        phrase = req.get_param('phrase') or '*'
        size = req.get_param('size') or 25
        resource_type = req.get_param('resource') or None
        if resource_type:
            resp.body = json.dumps(self.search_index.search(
                q=phrase,
                doc_type=resource_type,
                size=size))
        else:
            resp.body = json.dumps(self.search_index.search(
                q=phrase,
                size=size))
        resp.status = falcon.HTTP_200

    def on_patch(self, req, resp):
        """Method takes either sparql statement or predicate and object 
        and updates the Resource.

        Args:
            req -- Request
            resp -- Response
        """
        doc_uuid = req.get_param('uuid')
        if not doc_uuid:
            raise falcon.HTTPMissingParam('uuid')
        predicate = req.get_param('predicate') or None
        if not predicate:
            raise falcon.HTTPMissingParam('predicate')
        object_ = req.get_param('object') or None
        if not object_:
            raise falcon.HTTPMissingParam('object')
        doc_type = req.get_param('doc_type') or None
        if self.__update__(
            doc_id=doc_uuid,
            doc_type=doc_type,
            field=predicate,
            value=object_):
            resp.status = falcon.HTTP_202
            resp.body = json.dumps(True)
        else:
            raise falcon.HTTPInternalServerError(
                "Error with PATCH for {}".format(doc_uuid),
                "Failed setting {} to {}".format(
                    predicate,
                    object_))


class Repository(object):
    """Base repository object"""

    def __init__(self, config):
        """Initializes a Repository object.

        Arguments:
            config -- Configuration object
        """
        self.fedora = config['FEDORA']
        self.search = Search(config)
        admin = self.fedora.get('username', None)
        admin_pwd = self.fedora.get('password', None)
        self.config = config


    def __create__(self, **kwargs):
        """Internal method takes optional parameters and creates a new
        Resource in the Repository 

	keyword args:
            binary -- Binary object, any rdf will be stored as object's metadata 
            doc_type -- Elastic search document type, defaults to None
	    id -- Existing identifier defaults to None
            index -- Elastic search index, defaults to None
            mimetype -- Mimetype for binary stream, defaults to application/octet-stream
            rdf -- RDF graph of new object, defaults to None
            rdf_type -- RDF Type, defaults to text/turtle
        """
        pass

    def __new_property__(self, name, value):
        """Internal method adds a property to a Resource

        Args:
            name -- Name of property, should have correct prefix (i.e. bf, 
                    schema, fedora) 
            value -- value of property 
        Returns:
            boolean -- outcome of PATCH method call to repository 
        """
        pass

    def __replace_property__(self, name, current, new):
        """Internal method replaces a property (predicate) of the 
        Resource with a new value.

        Args:
            name -- Property name, should have correct prefix 
                   (i.e. bf,  schema, fedora) 
            current -- current value of property 
            new -- new value of property
        """
        pass

    def on_get(self, req, resp, id):
        """GET Method response, returns JSON, XML, N3, or Turtle representations
        of Entity
	    Args:
            req -- Request
            resp -- Response
	    id -- A unique ID for the Resource, should be UUID
        """
        pass


    def on_post(self, req, resp):
        """POST Method response, accepts optional binary file and RDF as
        request parameters in the POST

        Args:
            req -- Request
            resp -- Response
        """
        pass


    def on_delete(self, req, resp, id):
        """DELETE Method either deletes one or more predicate and objects from a
        Resource, or if both predicate and object are None, deletes the Resource
        itself. Should cascade through to Triplestore and Elasticsearch.

        Args:
            req -- Request
            resp -- Response
	    id -- A unique ID for the Resource, should be UUID
        """
        pass


    def on_get(self, req, resp, id):
        """GET Method response, returns JSON, XML, N3, or Turtle representations

        Args:
            req -- Request
            resp -- Response
            id -- A unique ID for the Resource, should be UUID
        """
        pass

    def on_put(self, req, resp, id):
        """PUT method takes an id, a list of predicate and object tuples and
        updates repository

        Args:
            req -- Request
            resp -- Response
            id -- Unique ID for the Resource
        """
        pass





