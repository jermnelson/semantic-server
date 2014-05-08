#-------------------------------------------------------------------------------
# Name:        api
# Purpose:     Provides a REST api for the Catalog Pull Platform's Semantic
#              Server
#
# Author:      Jeremy Nelson
#
# Created:     2014/05/01
# Copyright:   (c) Jeremy Nelson, Colorado College 2014
# Licence:     MIT
#-------------------------------------------------------------------------------
import redis

from bson import ObjectId
from flask import abort, Flask, request
from flask.ext.mongokit import Connection, MongoKit
from flask.ext.restful import Resource, Api
from elasticsearch import Elasticsearch

catalog = Flask(__name__)
catalog.config.from_pyfile('server.cfg')
api = Api(catalog)

es_search = Elasticsearch({'host': catalog.config.get(
                                       "ELASTICSEARCH_HOST",
                                       "localhost"),
                           'port': catalog.config.get(
                                       "ELASTICSEARCH_PORT",
                                       9500)})


mongo_ds  = Connection(host=catalog.config.get(
                           "MONGODB_HOST",
                           "localhost"),
                       port=catalog.config.get(
                           "MONGODB_PORT",
                           27017))

redis_ds = redis.StrictRedis(catalog.config.get(
                                 'REDIS_HOST',
                                 "localhost"),
                             catalog.config.get(
                                 'REDIS_PORT',
                                 6379))

class Entity(Resource):

    def __get_collection__(self, entity_type):
        """Internal method returns a MongoDB collection based on the type of
        Entity

        Args:
            entity_type (str): Type of entity

        Returns:
            collection (mongodb.Collection): MongoDB collection
        """
        if entity_type in mongo_ds.bibframe.collection_names():
            return getattr(mongo_ds.bibframe, entity_type)
        if entity_type in mongo_ds.schema_org.collection_names():
            return getattr(mongo_ds.schema_org, entity_type)
        if entity_type in mongo_ds.marc.collection_names():
            return getattr(mongo_ds.marc, entity_type)

    def get(self, entity_type, entity_id):
        # First try Redis Cache
        try:
            redis_key = 'result-cache:{}'.format(entity_id)
            if redis_ds.exists(redis_key):
                return {work_id: redis_ds.get(redis_key)}
        except redis.ConnectionError, e:
            # Redis not available
            pass
        collection = self.__get_collection__(entity_type)
        entity = collection.find_one({"_id": ObjectId(entity_id)})
        if entity is None:
            return abort(404)
        else:
            entity['mongo-id'] = str(entity.pop("_id"))
            return {entity_id: entity}


class Work(Entity):

    def get(self, work_id):
        # First checks MARC bibliographic
        entity = mongo_ds.marc.bibliographic.find_one(
            {'_id': ObjectId(work_id)})
        if entity is None:
            collection = self.__get_collection__('Work')
            entity = collection.find_one({"_id": ObjectId(work_id)})
        if entity is None:
            return abort(404)
        else:
            entity['mongo-id'] = str(entity.pop("_id"))
            return {work_id: entity}

api.add_resource(Work, '/Work/<string:work_id>')
api.add_resource(Entity, '/<string:entity_type>/<string:entity_id>')


def main():
    host = '0.0.0.0'
    catalog.run(debug=True,
                host=host)

if __name__ == '__main__':
    main()
