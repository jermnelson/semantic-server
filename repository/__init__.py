"""Module wraps Fedora Commons repository, Elasticsearch, and Fuseki
into single API for use in such projects as the BIBFRAME Catalog, TIGER Catalog,
Islandora eBadges, Schema.org Editor, and Django BFE projects.
"""
__author__ = "Jeremy Nelson"

import falcon
import json
import rdflib
import urllib.request

from elasticsearch import Elasticsearch
from .utilities.fuseki import Fuseki

class Search(object):
    """Search Repository"""

    def __init__(self, search_index, triplestore):
        self.search_index = search_index
        self.triplestore = triplestore

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


    def on_post(self, req, resp):
        return

    def url_from_id(self, id):
        return fedora_url

class Repository(object):
    """Base repository object"""

    def __init__(self, **kwargs):
        """Initializes a Repository object.

        Keyword arguments:
            es -- Elastic search instance, default is http://localhost:9200
            fuseki -- Fuseki instance, default is http://localhost:3030
            fedora -- Fedora 4 REST url, default is http://localhost:8080/rest/
            fedora3 -- Fedora 3.+ url, default is
            admin_user -- Fedora Administrator, defaults to None
            admin_pwd -- Fedora Password, defaults to None
        """
        self.fedora = kwargs.get('fedora', None)
        self.fedora3 = kwargs.get('fedora3', None)
        self.triple_store = kwargs.get('fuseki', Fuseki())
        self.search = Search(
            kwargs.get('es', Elasticsearch()),
            self.triple_store)

        if self.fedora and self.fedora3:
            raise ValueError("Cannot initialize both Fedora 3.+ {} and "\
                             "Fedora 4 {} in the same repository".format(
                             self.fedora3,
                             self.fedora))
        # Default is a Fedora 4 repository
        if not self.fedora and not self.fedora3:
            self.fedora = "http://localhost:8080/rest/"
        admin = kwargs.get('admin_user', None)
        admin_pwd = kwargs.get('admin_pwd', None)
        self.opener = None
        # Create a Password manager
        if admin and admin_pwd:
            password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
            password_mgr.add_password(
                None,
                self.fedora,
                admin,
                admin_pwd)
            handler = urllib.request.HTTPBasicAuthHandler(password_mgr)
            self.opener = urllib.request.build_opener(handler)





