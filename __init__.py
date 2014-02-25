#-------------------------------------------------------------------------------
# Name:        mongodb
# Purpose:     Module provides access to a MongoDB server
#
# Author:      Jeremy Nelson
#
# Created:     2014-01-22
# Copyright:   (c) Jeremy Nelson, Colorado College 2014
# Licence:     MIT
#-------------------------------------------------------------------------------
import datetime
import os
import pymarc
import sys
import urllib, urllib2

from bson.objectid import ObjectId
from gridfs import GridFS
from pymongo.errors import InvalidId

#from flask_bibframe.models import CoverArt

def generate_record_info(content_source, origin_msg):
    return {u'languageOfCataloging': u'http://id.loc.gov/vocabulary/iso639-1/en',
            u'recordContentSource': content_source,
            u'recordCreationDate': datetime.datetime.utcnow().isoformat(),
            u'recordOrigin': origin_msg}

def get_cover_art_image(db, entity_id):
    """Function returns the cover art image of an entity

    Args:
        db : MongoDB database
            MongoDB database with cover art and gridfs collections
        entity_id : str
            MongoDB _id for the entity
    """
    cover_art_collection = db.CoverArt
    cover_art_grid = GridFS(db)
    cover_art = cover_art_collection.find_one(
       { "annotates": ObjectId(entity_id)})
    if not cover_art:
        return
    image_id = ObjectId(cover_art.get('coverArtThumb'))
    if cover_art_grid.exists(image_id):
        return cover_art_grid.get(image_id)

def get_google_thumbnail(isbn):
    params = urllib.urlencode({'q': 'isbn:{}'.format(isbn),
                               'key': GBS_API_KEY})
    opener = urllib2.build_opener()
    opener.addheaders = [('User-agent', 'Mozilla/5.0')]
    result = json.load(urllib2.urlopen(google_book_url.format(params)))
    if int(result.get('totalItems')) < 1:
        return
    first_item = result.get('items')[0]
    if 'imageLinks' in first_item.get('volumeInfo'):
        if 'thumbnail' in first_item['volumeInfo']['imageLinks']:
            raw_image = opener.open(
                first_item['volumeInfo']['imageLinks']['thumbnail']).read()
            return raw_image

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
    marc_records = db.marc_records
    try:
        if type(marc_id) != ObjectId:
            marc_id = ObjectId(marc_id)
        return marc_records.find_one({'_id': marc_id})
    except InvalidId, e:
        return

def insert_cover_art(marc_collection,
                     bibframe_db,
                     limit=50000):
    """Searches Google Books for Thumbnail cover art using ISBN of MARC record

    Function takes a source Mongo collection along with a BIBFRAME Mongo
    DB, iterates through all of the records in the source collection, extracts
    the item's ISBN, and then attempts to harvest a thumbnail image from the a
    REST API call to Google Books.

    Args:
        marc_collection: MongoDB Collection of MARC21 documents
        bibframe_db: A MongoDB database of BIBFRAME entities
        limit: Number of MARC documents to process, default is 50,000

    Returns:
        None

    Raises:
        None
    """

    GBS_URL = 'https://www.googleapis.com/books/v1/volumes?{0}'
    cover_art_col = bibframe_db.CoverArt
    cover_art_grid = GridFS(bibframe_db)

    for row in marc_collection.marc_records.find({},
        {"fields.020.subfields.a": 1}).limit(limit):
        for field in row.get('fields'):
            if len(field) > 0:
                mongo_id = row.get('_id')
                if bibframe_db.find_one({"annotates": str(mongo_id)}):
                    continue
                isbn = field['020']['subfields'][0]['a']
                raw_image = get_google_thumbnail(isbn)
                if raw_image is not None:
                    image_id = cover_art_grid.put(raw_image)
                    cover_art = CoverArt(
                        annotates=str(mongo_id),
                        annotationSource=annotation_src_url,
                        assertionDate=datetime.datetime.utcnow().isoformat(),
                        coverArtThumb=str(image_id))
                    cover_art_dict = cover_art.as_dict()
                    cover_art_dict['recordInfo'] = generate_record_info(
                        "CoCCC",
                        "Harvested from Google Books")
                    cover_art_col.insert(cover_art.as_dict())


def insert_marc_file(collection, marc_filepath):
    """
    Function takes a MongoDB instance and a filepath and name to a MARC21 file
    and inserts a dict representation into the MongoDB marc_records collection.

    Args:
        collection: Flask-MongoKit Database Collection
        marc_filepath: Filepath and name to a MARC21 file

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
    print("Started insert {} into MongoDB at {}".format(marc_filepath,
                                                        start.isoformat()))
    for i, row in enumerate(marc_reader):
        if not i%100:
            sys.stderr.write(".")
        if not i%1000:
            sys.stderr.write(" {} ".format(i))
        record = row.as_dict()
        record['recordInfo'] = generate_record_info(
                        "CoCCC",
                        u'From Colorado College MARC21 records')
        marc_records.insert(record)
    end = datetime.datetime.now()
    end_count = marc_records.count()
    print("Finished {} in {} min".format(end.isoformat(),
                                     (end-start).seconds/60.0))
    print("Insert {} documents".format(end_count-start_count))





