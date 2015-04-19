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
    "fcrepo": str(FCREPO),
    "fedora": str(FEDORA),
    "fedoraconfig": str(FEDORACONFIG),
    "fedorarelsext": str(FEDORARELSEXT),
    "foaf": str(FOAF),
    "image": str(IMAGE),
    "indexing": "http://fedora.info/definitions/v4/indexing#",
    "mads": str(MADS),
    "mix": str(MIX),
    "mode": "http://www.modeshape.org/1.0",
    "owl": "http://www.w3.org/2002/07/owl#",
    "nt": "http://www.jcp.org/jcr/nt/1.0",
    "premis": "http://www.loc.gov/premis/rdf/v1#",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "schema": "http://schema.org/",
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
        predicate=FCREPO.uuid))

    TripleStore(resource.config).__load__(fuseki_sparql)

def ingest_turtle(graph):
    subject = next(graph.subjects(predicate=rdflib.RDF.type))
    raw_turtle = graph.serialize(format='turtle').decode()
    turtle = raw_turtle.replace("<{}>".format(subject), "<>")
    turtle = turtle[:-3]
    turtle += ";\n    owl:sameAs <{}> .\n\n".format(subject)
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
        self.search_index = Elasticsearch(
            [{"host": config["ELASTICSEARCH"]["host"],
              "port": config["ELASTICSEARCH"]["port"]}])
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
            return value.get('@id')
            #! Need to query triplestore?
	    #if uri in self.uris2uuid:
	    #    return self.uris2uuid[uri]
	    #else:
	    #    return uri
        return value


    def __generate_body__(self, graph, prefix=None):
        self.body = dict()
        graph_json = json.loads(
            graph.serialize(
                format='json-ld',
                context=CONTEXT).decode())
        if '@graph' in graph_json:
            for graph in graph_json.get('@graph'):
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
                                if prefix:
                                    if name.startswith(prefix):
                                        self.__set_or_expand__('type', name)
                                else:
                                    self.__set_or_expand__('type', name)
                        elif key.startswith('@id'):
                            self.__set_or_expand__('fcrepo:hasLocation', val)
                        elif not key.startswith('fcrepo') and not key.startswith('owl'):
                            self.__set_or_expand__(key, val) 

#    def __generate_suggestion__(self, subject, graph):
#        """Helper method should be overridden by child classes, used for autocorrection 
#        in Elastic search 
#
#        Args:
#            subject -- RDF Subject
#            graph -- rdflib.Graph
#        """
#        pass

    def __index__(self, subject, graph, doc_type, index): 
        self.__generate_body__(graph)
        doc_id = str(graph.value(
                     subject=subject,
                     predicate=FCREPO.uuid))
        self.__generate_suggestion__(subject, graph, doc_id)
        print("Attempting to index {} with id={}".format(
        subject, doc_id))
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
            self.body[key] = [self.__get_id_or_value__(value),]

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
            self.triplestore.__update__(result[0]['value'], field, value)         
            

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
                                   
        
             
          
            
        

    def url_from_id(self, id):
        return fedora_url

class Repository(object):
    """Base repository object"""


    def __init__(self, config):
        """Initializes a Repository object.

        Arguments:
            config -- Configuration object
        """
        if 'FEDORA3' in config:
            admin = self.config.get('FEDORA3', 'username')
            admin_pwd = self.config.get('FEDORA3', 'password')

        self.fedora = config['FEDORA']
        self.search = Search(config)
        admin = self.fedora.get('username', None)
        admin_pwd = self.fedora.get('password', None)
        self.opener = None
        # Create a Password manager
        if admin and admin_pwd:
            password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
            password_mgr.add_password(
                None,
                "{}:{}".format(self.fedora['host'], self.fedora['port']),
                admin,
                admin_pwd)
            handler = urllib.request.HTTPBasicAuthHandler(password_mgr)
            self.opener = urllib.request.build_opener(handler)
        self.config = config



    def __open_request__(self, fedora_request):
        """Internal method takes a urllib.request.Request and attempts to open
        the request with either the opener or direct urlopen call and then
        returns the result as a string.

        Args:
            fedora_request -- urllib.request.Request
        Returns:
            str
        """
        if self.opener:
            result = self.opener(fedora_request)
        else:
            result = urllib.request.urlopen(fedora_request)
        return result.read().decode()






