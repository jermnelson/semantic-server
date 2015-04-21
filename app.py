"""
The Semantic Server app module runs a number of different REST repository
services following a Falcon REST api design pattern. This endpoint
is meant to be expanded by implementing projects like the  BIBFRAME
Datastore Project (https://github.com/jermnelson/BIBFRAME-Datastore).
"""
__author__ = "Jeremy Nelson"

import configparser
import falcon
import json
import logging
import os

CURRENT_DIR = os.path.dirname(__file__)
BASE_DIR = os.path.dirname(CURRENT_DIR)

from werkzeug.serving import run_simple

try:
    from .repository import Info, Search
    from .repository.resources.fedora import Resource, Transaction
    from .repository.resources.fedora3 import FedoraObject
    from .repository.resources.fuseki import TripleStore
    from .repository.resources.islandora import IslandoraDatastream
    from .repository.resources.islandora import IslandoraObject
    from .repository.resources.islandora import IslandoraRelationship
    ##from .repository.resources.fuseki import TripleStore
    from .repository.utilities.migrating.foxml import FoxmlContentHandler
except (SystemError, ImportError):
    from repository import Info, Search
    from repository.resources.fedora import Resource, Transaction
    from repository.resources.fedora3 import FedoraObject
    from repository.resources.fuseki import TripleStore
    from repository.resources.islandora import IslandoraDatastream
    from repository.resources.islandora import IslandoraObject
    from repository.resources.islandora import IslandoraRelationship
    ##from repository.resources.fuseki import TripleStore
    from repository.utilities.migrating.foxml import FoxmlContentHandler


def set_version():
    "Simple function sets the API's version based on VERSION file"
    version_path = os.path.join(os.path.dirname(__file__), "VERSION")
    if not os.path.exists(version_path):
        version_path = os.path.join(BASE_DIR, "VERSION")
    if os.path.exists(version_path):
        with open(version_path) as version:
            return version.read().strip()
    else:
        return 'ERROR'

__version__ = set_version()

def load_config(config):
    """The `load_config` function takes a ConfigParser instance and attempts
    to load first a server.cfg file first in the BASE directory and then 
    the current directory before onto the default.cfg file in both locations.

    Args:
        config -- configparse.ConfigParser instance
    """ 
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


config = configparser.ConfigParser()
load_config(config)

class Config(object):

    def on_get(self, req, resp):
        output = {}
        for section in config.sections():
            items = {}
            for row in config.items(section):
                items[row[0]] = row[1]
            output[section] = items
        resp.status = falcon.HTTP_200
        resp.body = json.dumps(output)

class Version(object):

    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200
        resp.body = json.dumps({"version": __version__})

api = application = falcon.API()

api.add_route("/config", Config())
api.add_route("/info", Info(config))
api.add_route("/search", Search(config))
api.add_route("/version", Version())
if 'FEDORA' in config:
    resource = Resource(config)
    api.add_route("/Resource/", resource)
    api.add_route("/Resource/{id}", resource)
    api.add_route("/Transaction", Transaction(config))

if 'FEDORA3' in config:
    api.add_route("/migrate/foxml", FoxmlContentHandler(config))
    api.add_route("/Object/{pid}", FedoraObject(config))

if 'FUSEKI' in config: 
    api.add_route("/triplestore/", TripleStore(config))

if 'ISLANDORA' in config:
    islandora_object = IslandoraObject(config)
    islandora_datastream = IslandoraDatastream(config)
    islandora_relationship = IslandoraRelationship(config)
    api.add_route("/islandora/", islandora_object)
    api.add_route("/islandora/{pid}", islandora_object)
    api.add_route(
        "/islandora/{pid}/datastream/",
        islandora_datastream)
    api.add_route(
        "/islandora/{pid}/datastream/{dsid}",
        islandora_datastream)
    api.add_route(
        "/islandora/{pid}/relationship",
        islandora_relationship)

def main():
    "Simple main function runs wsgi container"
    debug = config.getboolean('REST_API', 'debug') or False
    logging.basicConfig(
        filename= config.get('LOGGING', 'filename'),
        level=int(config.get('LOGGING', 'level')))
    run_simple(
        config.get('REST_API', 'host'),
        config.getint('REST_API', 'port'),
        application,
        use_reloader=debug)

if __name__ == "__main__":
    main()
