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

class Info(object):
    """Basic information about available repository services"""

    def __init__(self, config):
        self.config = config

    def on_get(self,  req, resp):
        resp.status = falcon.HTTP_200
        resp.body = json.dumps(
            {"services": str(self.config)}
        )



class Search(object):
    """Search Repository"""

    def __init__(self, config):
        self.search_index = Elasticsearch(
            [{"host": config["ELASTICSEARCH"]["host"],
              "port": config["ELASTICSEARCH"]["port"]}])
        self.triplestore = Fuseki(url="{}:{}".format(
            config["FUSEKI"]["host"],
            config["FUSEKI"]["port"]))

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

    def __init__(self, config):
        """Initializes a Repository object.

        Keyword arguments:
            config -- Configuration object
        """
        if 'FEDORA3' in config:
            admin = config.get('FEDORA3', 'username')
            admin_pwd = config.get('FEDORA3', 'password')

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





