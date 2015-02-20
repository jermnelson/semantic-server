__author__ = "Jeremy Nelson"

import falcon
import json
import rdflib
import urllib.request

class Fuseki(object):

    def __init__(self, config):
        url = "http://{}:{}".format(
                config["FUSEKI"]["host"],
                config["FUSEKI"]["port"])
        datastore = config["FUSEKI"]["datastore"]
        self.update_url = "/".join([url, datastore, "update"])
        self.query_url = "/".join([url, datastore, "query"])

##    def on_post(self, req, resp):
##        rdf = req.get_param('rdf') or None
    def __load__(self, rdf):
        update_request = urllib.request.Request(
            self.update_url,
            method="POST",
            data=rdf.encode(),
            headers={"Accept": "text/xml"})
        result = urllib.request.urlopen(update_request)

    def on_put(self, req, resp):
        rdf = req.get_param('rdf') or None
        if rdf:
            self.__load__(self. rdf)
            msg = "Successfully loaded RDF into Fuseki"
        else:
            msg = "No RDF to load into Fuseki"
        resp.status = falcon.HTTP_200
        resp.body = json.dumps({"message": msg})



