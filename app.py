__author__ = "Jeremy Nelson"

global __version__
global config

import os
import configparser
import falcon

CURRENT_DIR = os.path.dirname(__file__)
BASE_DIR = os.path.dirname(CURRENT_DIR)

from elasticsearch import Elasticsearch
from werkzeug.serving import run_simple

try:
    from .repository import Info, Search
    from .repository.resources.fedora import Resource, Transaction
    from .repository.resources.fedora3 import FedoraObject
    from .repository.resources.islandora import IslandoraDatastream
    from .repository.resources.islandora import IslandoraObject
    from .repository.resources.islandora import IslandoraRelationship
    from .repository.utilities.fuseki import Fuseki
    from .repository.utilities.migrating.foxml import FoxmlContentHandler
except (SystemError, ImportError):
    from repository import Info, Search
    from repository.resources.fedora import Resource, Transaction
    from repository.resources.fedora3 import FedoraObject
    from repository.resources.islandora import IslandoraDatastream
    from repository.resources.islandora import IslandoraObject
    from repository.resources.islandora import IslandoraRelationship
    from repository.utilities.fuseki import Fuseki
    from repository.utilities.migrating.foxml import FoxmlContentHandler

api = None

def set_version():
    version_path = os.path.join(os.path.dirname(__file__), "VERSION")
    if not os.path.exists(version_path):
        version_path = os.path.join(BASE_DIR, "VERSION")
    if os.path.exists(version_path):
        with open(version_path) as version:
            __version__ = version.read().strip()
    else:
        __version__ = 'ERROR'

def load_config(config):
    config_filepath = os.path.join(BASE_DIR, 'server.cfg')
    if not os.path.exists(config_filepath):
        config_filepath = os.path.join(CURRENT_DIR, 'server.cfg')
    if not os.path.exists(config_filepath):
        config_filepath = os.path.join(BASE_DIR, 'default.cfg')
    if not os.path.exists(config_filepath):
        config_filepath = os.path.join(CURRENT_DIR, 'default.cfg')
    if not os.path.exists(config_filepath):
        raise ValueError(
            "Default configuration {} cannot be loaded".format(config_filepath))
    config.read(config_filepath)


set_version()

config = configparser.ConfigParser()
load_config(config)

api = application = falcon.API()

api.add_route("/info", Info(config))
api.add_route("/search",
    Search(config))
api.add_route("/Resource/", Resource(config))
api.add_route("/Resource/{id}", Resource(config))
api.add_route("/Transaction", Transaction(config))

if 'FEDORA3' in config:
    api.add_route("/migrate/foxml", FoxmlContentHandler(config))
    api.add_route("/Object/{pid}", FedoraObject(config))

if 'ISLANDORA' in config:
    api.add_route("/islandora/{pid}", IslandoraObject(config, pid))
    api.add_route(
        "/islandora/{pid}/datastream",
        IslandoraDatastream(config, pid))
    api.add_route(
        "/islandora/{pid}/relationship",
        IslandoraRelationship(config, pid))

def main():
    run_simple(
        config.get('DEFAULT', 'host'),
        config.getint('DEFAULT', 'port'),
        application,
        use_reloader=True)

if __name__ == "__main__":
    main()