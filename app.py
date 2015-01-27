__author__ = "Jeremy Nelson"

import falcon

from elasticsearch import Elasticsearch

from repository import Search
from repository.resources.fedora import Resource, Transaction
from repository.resources.fedora3 import FedoraObject
from repository.utilities.migrating.foxml import FoxmlContentHandler

from werkzeug.serving import run_simple
api = application = falcon.API()

api.add_route("/search", Search(Elasticsearch(), None))
api.add_route("/migrate/foxml", FoxmlContentHandler())

#
api.add_route("/Object/{pid}", FedoraObject())
api.add_route("/Resource/{id}", Resource())
api.add_route("/Transaction", Transaction())


if __name__ == '__main__':
    run_simple('0.0.0.0', 9001, application, use_reloader=True)
