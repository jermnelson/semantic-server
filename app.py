__author__ = "Jeremy Nelson"

import configparser
import falcon

from elasticsearch import Elasticsearch

from repository import Info, Search
from repository.resources.fedora import Resource, Transaction
from repository.resources.fedora3 import FedoraObject
from repository.utilities.migrating.foxml import FoxmlContentHandler



from werkzeug.serving import run_simple
api = application = falcon.API()

config = configparser.ConfigParser()
config.read('server.cfg')
if len(config) == 1: # Empty or nonexistent configuration, loads default
    config.read('default.cfg')

api.add_route("/info", Info(config))
api.add_route("/migrate/foxml", FoxmlContentHandler())
api.add_route("/search",
    Search(Elasticsearch([config['ELASTICSEARCH']]), None))

#
api.add_route("/Object/{pid}", FedoraObject())
api.add_route("/Resource/{id}", Resource())
api.add_route("/Transaction", Transaction())


if __name__ == '__main__':
    run_simple('0.0.0.0', 9001, application, use_reloader=True)
