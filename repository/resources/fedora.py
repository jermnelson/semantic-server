__author__ = "Jeremy Nelson"
import falcon
import json
import urllib.request
import urllib.parse
from .. import Repository, ingest_resource

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
        resource_uri = None
        # Create a new Resource based on request
        if resource_id:
            post_url = "/".join([self.rest_url, resource_id])
        else:
            post_url = self.rest_url
        # First check and add binary datastream
        if binary:
            fedora_add_request = urllib.request.Request(
                post_url,
                method="POST",
                data=binary)
            resource_uri = self.___open_request__(fedora_add_request)
            post_url =  "/".join([resource_uri, "fcr:metadata"])
        # Next handle any attached RDF
        if rdf:
            rdf_type = req.get_param('rdf-type') or "text/turtle"
            fedora_add_request = urllib.request.Request(
                post_url,
                data=rdf.encode(),
                method="POST",
                headers={"Content-type": rdf_type})
            rdf_url = self.__open_request__(fedora_add_request)
            # Binary URL is the resource_uri and not the fcr:metadata url
            if not resource_uri:
                resource_uri = rdf_url
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
