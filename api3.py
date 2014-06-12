#-------------------------------------------------------------------------------
# Name:        api3
# Purpose:     Provides a REST api for the Catalog Pull Platform's Semantic
#              Server using Python 3 and Fedora 4.
#
# Author:      Jeremy Nelson
#
# Created:     2014/06/12
# Copyright:   (c) Jeremy Nelson, Colorado College 2014
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import json
from flask import abort, Flask, request
from flask.ext.restful import Resource, Api
##from flask_fedora_commons import FedoraCommons
import sys
sys.path.append("C:\\Users\\jernelson\\Development\\flask-fedora\\")
from flask_fedora_commons.repository import Repository


catalog = Flask(__name__)
catalog.config.from_pyfile('server.cfg')
api = Api(catalog)
fedora = Repository()
##fedora = FedoraCommons(catalog)


class Entity(Resource):

   def get(self, entity_type, entity_id):
        """Method queries Fedora for entity's type and id returns 404 error
        if the entity is not found in Fedora.

        Args:
            entity_type(string): Type or Class of the entity
            entity_id(string): Unique id within the entity type's scope.

        Returns:
            dict: Dictionary of key-value or predicate-object for this entity.
        """
        entity = fedora.read("{}/{}".format(entity_type, entity_id))
        if entity is None:
            return abort(404)
        else:
            raw_json = json.loads(
                entity.serialize(format='json-ld').decode('utf-8'))
            return {entity_id: raw_json}



api.add_resource(Entity, '/<string:entity_type>/<string:entity_id>')

api = Api(catalog)

def main():
    host = '0.0.0.0'
    catalog.run(debug=True,
                host=host)

if __name__ == '__main__':
    main()
    print("Python 3.x api")