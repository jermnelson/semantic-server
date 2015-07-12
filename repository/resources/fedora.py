__author__ = "Jeremy Nelson"
import falcon
import json
import requests
import rdflib
import urllib.request
import urllib.parse
from .. import Repository, Search, default_graph, generate_prefix  
from .. import create_sparql_insert_row, ingest_resource, ingest_turtle
from ..utilities.namespaces import *

PREFIX = generate_prefix()

NEW_SPARQL = """{}
INSERT DATA {{{{
  <{{url}}> {{name}} {{value}} 
}}}}""".format(PREFIX)    

REPLACE_SPARQL = """{}
DELETE {{{{
<{{url}}> <{{name}}> <{{old_value}}>
}}}} INSERT {{{{
<{{url}}> <{{name}}> <{{new_value}}>
}}}} WHERE {{{{
}}}}""".format(PREFIX)

def serialize(req, resp, resource):
    resp.body = json.dumps(req.context['rdf'])

def replace_property(resource_url, name, old_value, new_value):
    """Internal method replaces a resource's existing property with a
    new value.

    Args:
        resource_url -- Fedora URL for the resource
        name -- Name of property, should have correct prefix (i.e. bf, 
	        schema, fedora) 
        old_value -- Old value of property, must match existing property's
	        value
        new_value -- New value of property
    Returns:
        boolean -- outcome of PATCH method call to Fedora
    """
    sparql = REPLACE_SPARQL.format(
        url=resource_url,
        name=name,
        old_value=old_value,
        new_value=new_value)
    fedora_result = requests.patch(
         resource_url, 
         data=sparql, 
         headers={'Content-Type': 'application/sparql-update'})
    if fedora_result.status_code < 300:
        return True
    return False  



class Resource(Repository):
    """Fedora Resource wrapper, see
    https://wiki.duraspace.org/display/FEDORA40/Glossary#Glossary-Resource

    >> import fedora
    >> resource = fedora.Resource()
    """

    def __init__(self, config, searcher=None, url=None):
        super(Resource, self).__init__(config)
        self.rest_url = "{}/rest".format(self.fedora)
        if searcher is None:
            self.searcher = Search(config)
        else:
            self.searcher = searcher
        if url:
            self.subject = rdflib.URIRef(url)
            self.graph = default_graph()
            self.graph.parse(url)
            self.uuid = url.split("/")[-1]
        else:  
            self.graph, self.subject, self.uuid = None, None, None

    def __create__(self, **kwargs):
        """Internal method takes optional parameters and creates a new
        Resource in Fedora, stores resulting triples in Fuseki and indexes
        the Resource into Elastic Search

	keyword args:
            binary -- Binary object for the Fedora Object, metadata will
                be stored as metadata to Binary.
            doc_type -- Elastic search document type, defaults to None
	    id -- Existing identifier defaults to None
            index -- Elastic search index, defaults to None
            mimetype -- Mimetype for binary stream, defaults to application/octet-stream
            rdf -- RDF graph of new object, defaults to None
            rdf_type -- RDF Type, defaults to text/turtle
        """
        if self.uuid:
            description = """Cannot call Resource.__create__, 
Fedora object {} already exists""".format(self.uuid)
            raise falcon.HTTPConflict(
                "Fedora object already exists",
                description)
        binary = kwargs.get('binary', None)
        doc_type = kwargs.get('doc_type', None)
        ident = kwargs.get('id', None)
        index = kwargs.get('index', None)
        mimetype = kwargs.get('mimetype', 'application/octet-stream')
        rdf = kwargs.get('rdf', None)
        rdf_type = kwargs.get('rdf_type', 'text/turtle') 
        resource_url = None
        if ident:
            fedora_post_url = "/".join([self.rest_url, ident])
        else:
            fedora_post_url = self.rest_url
        # First check and add binary datastream
        if binary:
            resource_url = self.__new_binary__(
                fedora_post_url, 
                binary, 
                mimetype,
                rdf)
        # Next handle any attached RDF
        if rdf and not binary:
            resource_url = self.__new_by_rdf__(
                fedora_post_url, 
                rdf, 
                rdf_type )
         # Finally, create a stub Fedora object if not resource_uri
        if not resource_url:
             stub_result = requests.post(
                 fedora_post_url)
             resource_url = stub_result.text
        self.subject = rdflib.URIRef(resource_url)
        self.graph = default_graph()
        self.graph = self.graph.parse(resource_url)
        self.uuid = resource_url.split("/")[-1]
        if index:
            self.searcher.__index__(self.subject, self.graph, doc_type, index)
        print("Size of index {}".format(self.searcher.search_index.count()))
        self.searcher.triplestore.__load__(self.graph)
        return resource_url



    def __new_by_rdf__(self, post_url, rdf, rdf_type):
        # If rdf is a rdflib.Graph, attempt to serialize
        if type(rdf) == rdflib.Graph:
            rdf = ingest_turtle(rdf)
            rdf_type = 'text/turtle'
        rdf_result = requests.post(
                post_url,
                data=rdf,
                headers={"Content-type": rdf_type})
        if rdf_result.status_code > 399:
            raise falcon.HTTPInternalServerError(
                "Error adding rdf to {}".format(post_url),
                "Error adding rdf file {},error:\n{}".format(
                    post_url,
                    rdf_result.text))
        return rdf_result.text


    def __new_binary__(self, post_url, binary, mimetype, rdf=None):
        """Internal method takes a Fedora POST url and a binary file to 
        create a Fedora Object and returns the fcr:metadata URL for 
        adding the binary's associated metadata.

        Args:
            post_url -- Fedora POST url
            binary -- binary datastream
            mimetype -- datastream's mimetype
            rdf -- Attached RDF metadata for binary, default is None
        Returns:
            new url for binary datastream's metadata
        """
        # Using urllib.request for binary upload
        binary_request = urllib.request.Request(
            post_url,
            data=binary,
            headers={"Content-Type": mimetype})
        binary_result = urllib.request.urlopen(binary_request)
        if binary_result.status > 399:
            raise falcon.HTTPInternalServerError(
                "Error adding binary to {}".format(post_url),
                "Error adding binary file {},error:\n{}".format(
                    post_url,
                    binary_result.read()))
        metadata_url = "/".join([binary_result.read().decode(), "fcr:metadata"])
        if rdf:
            metadata_uri = rdflib.URIRef(metadata_url)
            metadata_rdf = default_graph()
            metadata_rdf.parse(metadata_url)
            for p, o in rdf.predicate_objects():
                metadata_rdf.add((metadata_uri, p, o))
            rdf_put_result = requests.put(
                metadata_url,
                data=metadata_rdf.serialize(format='turtle'),
                headers={"Content-Type": "text/turtle"})
            if rdf_put_result.status_code > 399:
                raise falcon.HTTPInternalServerError(
                     "Error adding rdf to {}".format(post_url),
                     "Error adding rdf file {},error:\n{}".format(
                        metadata_url,
                        rdf_put_result.text))
        return metadata_url
    
       
    def __new_property__(self, name, value, index=True):
        """Internal method adds a property to a Fedora Resource

        Args:
            name -- Name of property, should have correct prefix (i.e. bf, 
                    schema, fedora) 
            value -- value of property 
        Returns:
            boolean -- outcome of PATCH method call to Fedora
        """
        if not self.subject:
            raise falcon.HTTPServiceUnavailable(
                "Resource doesn't exist to add property",
                "Resource doesn't exist add property {} with value {}".format(
                    name, 
                    value))
        sparql = NEW_SPARQL.format(
            url=str(self.subject),
            name=name,
            value=value)
        
        fedora_result = requests.patch(
            str(self.subject),
            data=sparql,
            headers={'Content-Type': 'application/sparql-update'})
        if fedora_result.status_code < 300:
            if index:
                self.searcher.__update__(
                    doc_id=self.uuid, 
                    field=name, 
                    value=value)
            return True
        return False  
          
    def __replace_binary__(self, 
                           url, 
                           binary, 
                           content_type='application/octet-stream'):
        if url.endswith("fcr:metadata"):
            url = url.split("/fcr:metadata")[0]
        replace_result = requests.put(
            url,
            files={"file": binary},
            headers={"Content-Type": content_type})
        if replace_result.status_code < 400:
            return True
        return False
        
 
    def __replace_property__(self, name, current, new):
        """Internal method replaces a property (predicate) of the Fedora 
        Resource with a new value.

        Args:
            name -- Property name, should have correct prefix 
                   (i.e. bf,  schema, fedora) 
            current -- current value of property 
            new -- new value of property
        """
        if not self.subject:
            description = """Resource doesn't exist to replace property {} with 
current value of {} with new value of {}""".format(
                    name,
                    current, 
                    new)
            raise falcon.HTTPServiceUnavailable(
                "Resource doesn't exist to replace property",
                description)
        if replace_property(str(self.subject), name, current, new):
            self.searcher.__update__(doc_id=self.uuid,
                                     field=name, 
                                     value=new)
            



    def on_delete(self, req, resp, id):
        """DELETE Method either deletes one or more predicate and objects from a
        Resource, or if both predicate and object are None, deletes the Resource
        itself. Should cascade through to Triplestore and Elasticsearch.

        Args:
            req -- Request
            resp -- Response
	        id -- A unique ID for the Resource, should be UUID
        """
        fedora_url = self.search.get(id)
        predicate = req.get_param('predicate') or None
        object_ = req.get_param('object') or None
        # If both predicate and object are none, delete the Resource from the
        # repository
        if predicate is None and object_ is None:
            delete_request = urllib.request.Request(
                fedora_url,
                method='DELETE')
            result = urllib.request.urlopen(delete_request)
            return True

    @falcon.after(serialize)
    def on_get(self, req, resp, id):
        """GET Method response, returns JSON, XML, N3, or Turtle representations

	    Args:
            req -- Request
            resp -- Response
	        id -- A unique ID for the Resource, should be UUID
        """
        fedora_url = self.search.url_from_id(id)
        req.context['rdf'] = rdflib.Graph().parse(fedora_url)

    def on_patch(self, req, resp, id, sparql):
        fedora_url = self.search.url_from_id(id)

        fedora_url_request = urllib.request.Request(
            fedora_url,
            data=sparql.encode(),
            method='PATCH',
            headers={'Content-Type': 'application/sparql-update'}
        )

        if self.opener:
            result = self.opener(fedora_url_request)
        else:
            result = urllib.request.urlopen(fedora_url_request)
        resp.status = falcon.HTTP_200
        resp.body = json.dumps({"message": "{} updated".format(id)})

    @falcon.after(ingest_resource)
    def on_post(self, req, resp):
        """POST Method response, accepts optional binary file and RDF as
        request parameters in the POST

        Args:
            req -- Request
            resp -- Response
        """
        binary = req.get_param('binary') or None
        rdf = req.get_param('rdf') or None
        resource_id = req.get_param('id', None)
        if not resource_uri:
            fedora_add_request = urllib.request.Request(
                post_url,
                method="POST")
            resource_uri = self.__open_request__(fedora_add_request)
        resp.status = falcon.HTTP_201
        resp.body = json.dumps({
            "message": "Created Fedora Resource id={}".format(
                resource_uri.split("/")[-1]),
            "uri": resource_uri
            })



    def on_put(self, req, resp, id):
        """PUT method takes an id, a list of predicate and object tuples and
        updates Repository

        Args:
            req -- Request
            resp -- Response
            id -- Unique ID for the Resource
        """
        fedora_url = self.search.url_from_id(id)

        fedora_url_request = urllib.request.Request(
            fedora_url,
            data=sparql.encode(),
            method='PUT',
            headers={'Content-Type': 'application/sparql-update'}
        )
        if self.opener:
            result = self.opener(fedora_url_request)
        else:
            result = urllib.request.urlopen(fedora_url_request)
        return True

class Container(Resource):
    
    def __init__(self, config):
        super(Container, config).__init__(config)

    def on_get(self, req, resp):
        pass

    def on_post(self, req, resp):
        pass


class Transaction(Repository):

    def on_get(self, req, resp, token=None):
        pass
