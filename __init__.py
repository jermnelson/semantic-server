#-------------------------------------------------------------------------------
# Name:        mongo_datastore
# Purpose:     Module provides access to a MongoDB server
#
# Author:      Jeremy Nelson
#
# Created:     2014-01-22
# Copyright:   (c) Jeremy Nelson, Colorado College 2014
# Licence:     MIT
#-------------------------------------------------------------------------------
import datetime
import json
import os
import pymarc
import re
import sys
import urllib

from bson.objectid import ObjectId
from gridfs import GridFS
from flask import Blueprint, Config, current_app, g, render_template
from flask.ext.mongokit import Connection
from flask.ext.elasticsearch import ElasticSearch
##from flask_bibframe.models import CoverArt
from pymongo.errors import ConnectionFailure, InvalidId, OperationFailure

semantic_server = Blueprint('semantic_server',
                            __name__,
                            template_folder='templates')

blueprint_folder = os.path.abspath(os.path.dirname(__file__))
app_folder = os.path.split(blueprint_folder)[0]

semantic_server_config = Config(app_folder)
semantic_server_config.from_pyfile('catalog.cfg')

try:
    elastic_search = ElasticSearch(semantic_server)
    mongo_client = Connection(semantic_server_config.get('MONGODB_HOST'))

except ConnectionFailure:
    try:
        mongo_client = Connection()
        elastic_search = ElasticSearch()
    except ConnectionFailure:
        mongo_client = None

def check_for_cover_art(entity_id, db=mongo_client.bibframe):
    """Function checks if the entity has Cover Art

    Args:
        entity_id: String of Mongo ID of Entity
        db: BIBFRAME Mongo Database, defaults to bibframe

    Returns:
        boolean
    """
    cover_art_collection = db.CoverArt
    cover_art = cover_art_collection.find_one(
       { "annotates": entity_id})
    if cover_art:
        return True
    else:
        return False




def get_cover_art_image(entity_id, db=mongo_client.bibframe):
    """Function returns the cover art image of an entity

    Args:
        entity_id: str
            MongoDB _id for the entity
        db: MongoDB database
            MongoDB database with cover art and gridfs collections

    """
    cover_art_collection = db.CoverArt
    cover_art_grid = GridFS(db)
    cover_art = cover_art_collection.find_one(
       { "annotates": entity_id})
    if not cover_art:
        return
    image_id = ObjectId(cover_art.get('coverArtThumb'))
    if cover_art_grid.exists(image_id):
        return cover_art_grid.get(image_id)

ISBN_RE = re.compile(r'(\b\d{10}\b|\b\d{13}\b)')
def get_google_thumbnail(isbn, gbs_api_key):
    """Function attempts to harvest thumbnail image from Google Book service

    Args:
        isbn: str
            ISBN of Work
        gbs_api_key: str
            Google Book Service API key

    Returns:
        An empty tuple or a tuple made up of URL and the raw image from Google
        Book Service
    """
    if ISBN_RE.search(isbn):
        isbn = ISBN_RE.search(isbn).groups()[0]
    google_book_url = 'https://www.googleapis.com/books/v1/volumes?{0}'
    params = urllib.urlencode({'q': 'isbn:{}'.format(isbn),
                               'key': gbs_api_key})
    opener = urllib2.build_opener()
    opener.addheaders = [('User-agent', 'Mozilla/5.0')]
    result = json.load(urllib2.urlopen(google_book_url.format(params)))
    if int(result.get('totalItems')) < 1:
        return (None, None)
    first_item = result.get('items')[0]
    if 'imageLinks' in first_item.get('volumeInfo'):
        if 'thumbnail' in first_item['volumeInfo']['imageLinks']:
            raw_image = opener.open(
                first_item['volumeInfo']['imageLinks']['thumbnail']).read()
            return (google_book_url.format(
                        urllib.urlencode({'q': 'isbn:{}'.format(isbn)})),
                    raw_image)
    return (None, None)


def get_item_details(mongo_id, mongo_client=mongo_client):
    """Returns rendered HTML of an item's details

    Function takes a mongo_id and returns a rendered template of the item. An
    item here is merely shorthand for either a MARC21 bibliographic record,
    a Schema.org Creative Work, or a BIBFRAME Instance.

    Args:
        mongo_id: Mongo ID as either a string or ObjectId
        mongo_client: Mongo Client, defaults to module's client

    Returns:
        HTML snippet

    Raises:
        ValueNotFound: if mongo_id is not present in the MongoDB datastore
    """
    item, output = {}, '<h4>Details</h4>'
    if not isinstance(mongo_id, ObjectId):
        mongo_id = ObjectId(mongo_id)
    # Try MARC Bibliographic Record Database
    marc_records = get_marc_records_collection(mongo_client)
    item = __marc_item__(mongo_client, mongo_id)
    # Try Schema.org Database
    if not item:
        schema_db = mongo_client.schema_org
        result = schema_db.CreativeWork.find_one(
            {"_id": mongo_id})
        if not result is None:
            item = result
    # Try BIBFRAME Database
    if len(item) < 1:
        bibframe_db = mongo_client.bibframe
        result = bibframe_db.Work.find_one(
            {"_id": mongo_id})
        if not result is None:
            item = result
    if len(item) > 0:
        output = render_template('mongo_datastore/item.html',
                                 item=item)
    return output


def get_marc(db, marc_id):
    """
    Function takes a MongoDB instance and a marc_id, returns Null or Dictionary
    of the MARC record.

    Args:
        db: Flask-MongoKit DB
        marc_id: str
            Mongo ID of MARC record, can take either string of hash or ObjectId

    Returns:
        None

    Raises:
        InvalidId: An error occured with an invalid binary JSON Object ID
    """
    marc_records = get_marc_records_collection(db)
    if marc_records is None:
        return
    try:
        if type(marc_id) != ObjectId:
            marc_id = ObjectId(marc_id)
        return marc_records.find_one({'_id': marc_id})
    except InvalidId:
        return

def get_work(db, work_id):
    """
    Function takes MongoDB instance, checks bibframe, schema.org, and MARC data
    collections and returns dictionary or Null

    Args:
        db: Flask-MongoKit DB
        work_id: str
            Mongo ID of Work record, can take either string of hash or ObjectId

    Returns:
        dict

    Raises:
        InvalidId: An error occured with an invalid binary JSON Object ID
    """
    # First try bibframe
    bibframe_db = db.bibframe
    bibframe_work = bibframe_db.Work.find_one(
        {'_id': ObjectId(work_id)})
    if bibframe_work:
        if '@type' not in bibframe_work:
            bibframe_work['@type'] = 'bf:Work'
        return bibframe_work
    # Next try schema.org
    schema_org_db = db.schema_org
    schema_work = schema_org_db.CreativeWork.find_one(
        {'_id': ObjectId(work_id)})
    if schema_work:
        if '@type' not in schema_work:
            schema_work['@type'] = 'schema:CreativeWork'
        return schema_work
    # Finally try with MARC
    marc_db = get_marc_records_collection(db)
    marc_work = marc_db.find_one({'_id': ObjectId(work_id)})
    if marc_work:
        marc_work['@type'] = 'MARC'
        return marc_work
    return




def get_marc_records_collection(client):
    for db in client.database_names():
        if 'marc_records' in getattr(client, db).collection_names():
            return getattr(client, db).marc_records

def insert_cover_art(marc_db,
                     bibframe_db,
                     gbs_api_key,
                     limit=50000):
    """Searches Google Books for Thumbnail cover art using ISBN of MARC record

    Function takes a source Mongo collection along with a BIBFRAME Mongo
    DB, iterates through all of the records in the source collection, extracts
    the item's ISBN, and then attempts to harvest a thumbnail image from the a
    REST API call to Google Books.

    Args:
        marc_db: MongoDB database of MARC21 document collections
        bibframe_db: A MongoDB database of BIBFRAME collections
        gbs_api_key: Google Book Service API key
        limit: Number of MARC documents to process, default is 50,000

    Returns:
        None

    Raises:
        None
    """
    cover_art_col = bibframe_db.CoverArt
    cover_art_grid = GridFS(bibframe_db)
    start_time = datetime.datetime.now()
    start_count = cover_art_col.count()
    print("Starting CoverArt ingestion {}".format(start_time.isoformat()))
    print("size of CoverArt Collection {}".format(start_count))
    for i, row in enumerate(marc_db.marc_records.find({},
        {"fields.020.subfields.a": 1}, timeout=False).limit(limit)):
        if not i%100:
            sys.stderr.write(".")
        if not i%1000:
            sys.stderr.write(" {} ".format(i))
        for field in row.get('fields'):
            if len(field) > 0:
                mongo_id = row.get('_id')
                if bibframe_db.CoverArt.find_one({"annotates": str(mongo_id)}):
                    continue
                if not 'a' in field['020']['subfields'][0]:
                    continue
                isbn = field['020']['subfields'][0]['a']
                try:
                    annotate_src_url, raw_image = get_google_thumbnail(isbn,
                                                                       gbs_api_key)
                except:
                    raw_image = None
                if raw_image is not None:
                    image_id = cover_art_grid.put(raw_image)
                    cover_art = CoverArt(
                        annotates=str(mongo_id),
                        annotationSource=annotate_src_url,
                        assertionDate=datetime.datetime.utcnow().isoformat(),
                        coverArtThumb=image_id)
                    cover_art_col.insert(cover_art.as_dict())

    end_time = datetime.datetime.now()
    end_count = cover_art_col.count()
    print("Finished {} in {} min".format(end_time.isoformat(),
                                         (end_time-start_time).seconds/60.0))
    print("Inserted {} CoverArt entitles".format(end_count-start_count))




def insert_marc_file(collection, marc_filepath, redis_ds):
    """
    Function takes a MongoDB instance and a filepath and name to a MARC21 file
    and inserts a dict representation into the MongoDB marc_records collection.

    Args:
        collection: Flask-MongoKit Database Collection
        marc_filepath: Filepath and name to a MARC21 file
        redis_ds: Redis Datastore

    Returns:
        None

    Raises:
        None
    """
    if not os.path.exists(marc_filepath):
        print(" {} not found".format(marc_filepath))
        return
    marc_records = collection
    marc_reader = pymarc.MARCReader(
        open(marc_filepath, 'rb'),
        to_unicode=True)
    start = datetime.datetime.now()
    start_count = marc_record.count()
    errors = []
    print("Started insert {} into MongoDB at {}".format(marc_filepath,
                                                        start.isoformat()))
    for i, row in enumerate(marc_reader):
        if not i%100:
            sys.stderr.write(".")
        if not i%1000:
            sys.stderr.write(" {} ".format(i))
        legacy_bib_number = record['907']['a']
        if redis_ds.hexists('legacy-bib-num', legacy_bib_number):
            continue
        record = row.as_dict()
        record['recordInfo'] = generate_record_info(
                        "CoCCC",
                        u'From Colorado College MARC21 records')
        object_id = marc_records.insert(record)
        redis_ds.hset('legacy-bib-num', legacy_bib_number, str(object_id))
    end = datetime.datetime.now()
    end_count = marc_records.count()
    print("Finished {} in {} min".format(end.isoformat(),
                                     (end-start).seconds/60.0))
    print("Insert {} documents".format(end_count-start_count))


def __marc_item__(marc_db, mongo_id):
    """Helper function returns a dictionary of an item from a MARC Mongo DB

    Function  iteriates through the various bibliographic and authority
    collections, extracts information for call number, publisher, pages,
    isbn/issn, and physical description for use in an item template

    Args:
        marc_db: MongoDB database with one or more MARC record collections
        mongo_id: MongoID of the entity

    Returns:
        dictionary with item information extracted from collections
    """
    item = {}
    marc = get_marc(marc_db, mongo_id)
    if marc is None:
        return
    for row in marc.get('fields', []):
        tag = row.keys()[0]
        if tag == '020':
            if 'a' in row[tag]['subfields']:
                item['isbn'] = row[tag]['subfields']['a']
        if tag == '050': # LC call number
            for subfield in row[tag]['subfields']:
                if 'a' in subfield:
                    item['lc_call_number'] = subfield['a']
                if 'b' in subfield:
                    item['lc_call_number'] = "{}{}".format(
                        item['lc_call_number'],
                        subfield['b'])
        if tag == '090':
            for subfield in row[tag]['subfields']:
                if 'a' in subfield:
                    local_call_number = subfield['a']
                    if not item.get('lc_call_number') == local_call_number:
                        item['local_call_number'] = local_call_number
        if tag == '260':
            subfields = row[tag]['subfields']
            for subfield in subfields:
                if 'a' in subfield:
                    item['place'] = subfield['a']
                if 'b' in subfield:
                    item['publisher'] = subfield['b']
                if 'c' in subfield:
                    item['datePublished'] = subfield['c']
        if tag == '300':
            subfields = row[tag]['subfields']
            for subfield in subfields:
                if 'a' in subfield:
                    extent = subfield['a']
                    if not 'pages' in item:
##                    item['extent'] = '{} {}'.format(item['extent'],
##                                                    extent)
##                else:
                        item['pages'] = extent
                if 'c' in subfield:
                    dimensions = subfield['c']
                    if 'description' in item:
##                    item['description'] = '{} {}'.format(item['description'],
##                                                         dimensions)
##                else:
                        item['description'] = dimensions
        if tag == '710':
            if 'a' in row[tag]['subfields']:
                item['publisher'] = row[tag]['subfields']['a']
    return item




