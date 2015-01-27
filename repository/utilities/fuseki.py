__author__ = "Jeremy Nelson"

import rdflib
import urllib.request

class Fuseki(object):

    def __init__(self, url="http://localhost:3030", datastore='ds'):
        self.update_url = "/".join([url, datastore, "update"])
        self.query_url = "/".join([url, datastore, "query"])