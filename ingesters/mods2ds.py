#-------------------------------------------------------------------------------
# Name:        mods2ds
# Purpose:     Ingests MODS into Linked Data schema.org and BIBFRAME entities in
#              the Catalog Pull Platform
#
# Author:      Jeremy Nelson
#
# Created:     2014-02-27
# Copyright:   (c) Jeremy Nelson, Colorado College 2014
# Licence:     MIT
#-------------------------------------------------------------------------------
import xml.etree.ElementTree as etree
import flask_bibframe.models as bf_models

from bson import ObjectId
from flask_schema_org.models import CreativeWork, Person, Organization

from catalog.mongo_datastore import generate_record_info

from rdflib import Namespace

FEDORA_M_NS = Namespace('http://www.fedora.info/definitions/1/0/management/')
MODS_NS = Namespace("http://www.loc.gov/mods/v3")

def get_or_add_person(name, client):
    """Function retrieves a schema:Person or adds a new schema:Person from MODS

    This function assumes that the mods name/namePart text is in the format of
    "familyName, givenName".

    Args:
        name: MODS name element
        client: Mongo DB Client

    Returns:
        ObjectId: Mongo DB ObjectId for the schema.org Person
    """
    schema_org = client.schema_org
    bibframe = client.bibframe
    name_type = name.attrib.get('type')
    if not name_type.startswith('personal'):
        return
    bf_person = bf_models.Person()
    nameParts = name.findall("{{{0}}}namePart".format(MODS_NS))
    if len(nameParts) == 0:
        return
    elif len(nameParts) == 1:
        existing_person = schema_org.Person.find_one({"name": nameParts[0].text},
                                                     {"_id": 1})
        if existing_person:
            return existing_person.get('_id')
        name_list = [part.strip() for part in namePart.text.split(",") if part.find('editor') < 0]
    else:
        name_list = []
        for part in nameParts:
            if part.attrib.get('type') == 'family':
                name_list




    person = Person(givenName=name_list[-1],
                    familyName=name_list[0],
                    name=namePart.text)
    # Ugh about hard-coding these values, should come from config
    person.recordInfo = generate_record_info(
                              u"CoCCC",
                              u'From Colorado College MODS record')
    person.sameAs = []
    person_id = schema_org.Person.insert(person.as_dict())
    bf_person.relatedTo = [str(person_id)]
    bf_person_id = bibframe.Person.insert(bf_person.as_dict())
    schema_org.Person.update({"_id": person_id},
                             {"$push": {'sameAs': str(bf_person_id)}})
    return person_id


def get_or_add_organization(name, client):
    """Existing organization or adds new Organization

    Args:
        name: MODS name element
        client: Mongo DB Client

    Returns:
        ObjectId: Mongo DB ObjectId for the schema.org Person
    """
    schema_org = client.schema_org
    bibframe = client.bibframe
    name_type = name.attrib.get('type')
    if not name_type.startswith('corporate'):
        return
    namePart = name.find("{{{0}}}namePart".format(MODS_NS))
    if namePart.text is None:
        return
    existing_org = schema_org.Organization.find_one({"name": namePart.text},
                                                    {"_id": 1})
    if existing_org:
        return existing_org.get("_id")
    organization = Organization(name=namePart.text)
    organization['recordInfo'] = generate_record_info(
                                     u"CoCCC",
                                     u'From Colorado College MODS record')

    new_org = schema_org.Organization.insert(organization.as_dict())
    bf_organization = bf_models.Organization(
        relatedTo=str(new_org.get('_id')),
        label=namePart.text,
        recordInfo=generate_record_info(
            u"CoCCC",
            u'From Colorado College MODS record'))
    bf_id = bibframe.Organization.insert(bf_organization.as_dict())
    schema_org.Organization.update({"_id": new_org},
                                   {"sameAs": [str(bf_organization.get('_id'))]})
    return new_org


def add_base(mods, client):
    """Adds common elements from MODS to their schema.org counterparts

    Args:
        mods: MODS XML etree
        client: Mongo DB Client

    Returns:
        dict: Dictionary of schema.org properties
    """
    schema_org = client.schema_org
    bibframe = client.bibframe
    instance = bf_models.Instance()
    output = {}
    # Process MODS name
    for name in mods.findall("{{{0}}}name".format(MODS_NS)):
        name_type = name.attrib.get('type', None)
        role = name.find("{{{0}}}role/{{{0}}}roleTerm".format(MODS_NS))
        if name_type == 'personal':
            person_id = get_or_add_person(name, client)
            if person_id and role:
                if role.text == 'creator':
                    if 'creator' in output:
                        output['creator'].append(str(person_id))
                    else:
                        output['creator'] = [str(person_id)]
    # Process MODS title
    title = mods.find("{{{0}}}titleInfo/{{{0}}}title".format(MODS_NS))
    if title is not None:
        output['headline'] = title.text
    # Process MODS originInfo
    originInfo = mods.find("{{{0}}}originInfo".format(MODS_NS))
    publisher = originInfo.find("{{{0}}}publisher".format(MODS_NS))
    if publisher and publisher.text:
        existing_org = schema_org.Organization.find_one(
                           {"name": publisher.text},
                           {"_id": 1})
        if existing_org:
            output['publisher'] = str(existing_org.get("_id"))
        else:
            organization = Organization(publisher.text)
            organization['recordInfo'] = generate_record_info(
                                            u"CoCCC",
                                            u'From Colorado College MODS record')
            output['publisher'] = str(schema_org.Organization.insert(
                                        organization.as_dict()))
        output['copyrightHolder'] = [output['publisher'],]

    dateIssued = originInfo.find("{{{0}}}dateIssued".format(MODS_NS))
    if dateIssued is not None:
        output['datePublished'] = dateIssued.text
    # Process MODS topics
    topics = mods.findall("{{{0}}}subject/{{{0}}}topic".format(MODS_NS))
    if len(topics) > 0:
        output['keywords'] = []
        for topic in topics:
            if topic.text:
                output['keywords'].append(topic.text)
    # Process MODS location
    location_url = mods.find("{{{0}}}location/{{{0}}}url".format(MODS_NS))
    if location_url is not None:
        output['url'] = location_url.text
        if output['url'].startswith("http://hdl.handle.net"):
            instance.hdl = output['url']
    return output



def add_publication_issue(mods, client):
    schema_org = client.schema_org
    bibframe = client.bibframe
    base_mods = add_base(mods, client)
    publication_issue = CreativeWork(**base_mods)
    publication_issue.additionalType = 'PublicationIssue'

def add_publication_issue(mods, client):
    schema_org = client.schema_org
    bibframe = client.bibframe
    base_mods = add_base(mods, client)
    publication_volume = CreativeWork(**base_mods)
    publication_volume.additionalType = 'PublicationVolume'

def add_periodical(mods, client):
    """Takes a MODS etree and adds a Periodical
    (as proposed http://www.w3.org/community/schemabibex/) to the Mongo
    Datastore

    Function takes a MODS etree and based on mods:genre value, creates a
    custom Thesis Schema.org class that is descendent from schema:CreativeWork

    Args:
        mods: MODS XML etree
        client: Mongo DB Client

    Returns:
        ObjectId: Mongo DB ObjectId for the schema.org Thesis
    """
    schema_org = client.schema_org
    bibframe = client.bibframe
    base_mods = add_base(mods, client)
    periodical = CreativeWork(**base_mods)
    periodical.additionalType = 'Periodical'




def add_thesis(mods, client):
    """Takes a MODS etree and adds a Thesis to the Mongo Datastore

    Function takes a MODS etree and based on mods:genre value, creates a
    custom Thesis Schema.org class that is descendent from schema:CreativeWork

    Args:
        mods: MODS XML etree
        client: Mongo DB Client

    Returns:
        ObjectId: Mongo DB ObjectId for the schema.org Thesis
    """
    schema_org = client.schema_org
    bibframe = client.bibframe
    base_mods = add_base(mods, client)
    thesis = CreativeWork(**base_mods)
    thesis.genre = 'thesis'
    thesis.copyrightHolder.extend(base_mods['creator'])
    bf_text = bf_models.Text(recordInfo=generate_record_info(),
                             title=base_mods.get('headline'))
    for name in mods.findall("{{{0}}}name".format(MODS_NS)):
        name_type = name.attrib.get('type')
        role = name.find("{{{0}}}role/{{{0}}}roleTerm".format(MODS_NS))
        if name_type == 'corporate':
            org_id = get_or_add_organization(name, client)
            if org_id and role:
                if role.text == 'sponsor':
                    thesis.sourceOrganization = str(org_id)
                    if thesis.publisher:
                        publisher = schema_org.Organization.find_one(
                            {'_id': ObjectId(thesis.publisher)})
                        if not str(org_id) in publisher.department:
                            schema_org.Organization.update(
                                {'_id': publisher.get('_id')},
                                { '$push': {"department", str(org_id)}})
    if thesis.publisher:
        bf_organization = bibframe.Organization.find_one(
            {"relatedTo": thesis.publisher},
            {"_id": 1})
        bf_text.dissertationInstitution = str(bf_organization.get('_id'))
    for note in mods.findall("{{{0}}}note".format(MODS_NS)):
        if note.attrib['type'] == 'thesis' and attrib.get('displayLabel') == "Degree Name":
            bf_text.dissertationDegree = note.text


def insert_mods(db, mods_xml, redis_ds):
    """Inserts a MODS XML datastream to MongoDB schema_org and bibframe
    collections.

    Args:
        db: Flask-MongoKit Database
        mods_xml: Raw MODS XML
        redis_ds: Redis Datastore

    Returns:
        None

    Raises:
        None
    """
    pass


























