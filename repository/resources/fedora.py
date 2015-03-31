__author__ = "Jeremy Nelson"
import falcon
import json
import requests
import rdflib
import urllib.request
import urllib.parse
from .. import Repository, Search, ingest_resource, ingest_turtle

def serialize(req, resp, resource):
    resp.body = json.dumps(req.context['rdf'])


class Resource(Repository):
    """Fedora Resource wrapper, see
    https://wiki.duraspace.org/display/FEDORA40/Glossary#Glossary-Resource

    >> import fedora
    >> resource = fedora.Resource()
    """

    def __init__(self, config):
        super(Resource, self).__init__(config)
        self.rest_url = "http://{}:{}/rest".format(
            self.fedora['host'],
            self.fedora['port'])
        self.searcher = Search(config)

    def __new_by_rdf__(self, post_url, rdf, rdf_type):
        # If rdf is a rdflib.Graph, attempt to serialize
        if type(rdf) == rdflib.Graph:
            if rdf_type.endswith('turtle'):
                rdf = rdf.serialize(format='turtle')
            elif rdf_type.endswith('nt'):
                rdf = rdf.serialize(format='nt')
            elif rdf_type.count('json') > 0: # various json variants
                rdf = rdf.serialize(format='json-ld')
            else: # default is xml
                rdf = rdf.serialize()
        rdf_result = requests.post(
                fedora_post_url,
                data=rdf,
                headers={"Content-type": rdf_type})
        if rdf_result.status_code > 399:
            raise falcon.HTTPInternalServerError(
                "Error adding rdf to {}".format(post_url),
                "Error adding rdf file {},error:\n{}".format(
                    post_url,
                    rdf_result.text))
        return rdf_result.text


    def __new_binary__(self, post_url, binary):
        binary_result = requests.post(
            fedora_post_url,
            data=binary)
        if binary_result.status_code > 399:
            raise falcon.HTTPInternalServerError(
                "Error adding binary to {}".format(post_url),
                "Error adding binary file {},error:\n{}".format(
                    post_url,
                    binary_result.text))
        return "/".join([binary_result.text, "fcr:metadata"])
       

    def __create__(self, **kwargs):
        """Internal method takes optional parameters and creates a new
        Resource in Fedora, stores resulting triples in Fuseki and indexes
        the Resource into Elastic Search

	keyword args:
            binary -- Binary object for the Fedora Object, metadata will
                be stored as metadata to Binary.
	    id -- Existing identifier defaults to None
            rdf -- RDF graph of new object, default to None
            rdf-type -- RDF Type, default to text/turtle
        """
        binary = kwargs.get('binary', None)
        ident = kwargs.get('id', None)
        rdf = kwargs.get('rdf', None)
        rdf_type = kwargs.get('rdf-type', 'text/turtle') 
        resource_url = None
        if ident:
            fedora_post_url = "/".join([self.rest_url, ident])
        else:
            fedora_post_url = self.rest_url
        # First check and add binary datastream
        if binary:
            resource_url = self.__new_binary__(fedora_post_url, binary)
        # Next handle any attached RDF
        if rdf:
            resource_url = self.__new_by_rdf__(fedora_post_url, rdf, rdf_type)
         # Finally, create a stub Fedora object if not resource_uri
        if not resource_url:
             stub_result = requests.post(
                 fedora_post_url)
             resource_url = stub_result.text
        subject = rdflib.URIRef(resource_url)
        graph = rdflib.Graph().parse(resource_url)
        self.searcher.__index__(subject, graph)
        self.searcher.__load__(graph)
        return resource_url


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
                # Finally, if neither binary or RDF, create a stub Fedora Resource
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



class Transaction(Repository):

    def on_get(self, req, resp, token=None):
        pass
