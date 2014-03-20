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
from flask_schema_org.models import AudioObject, CreativeWork, Map, Organization
from flask_schema_org.models import Person, Photograph, VideoObject

from catalog.mongo_datastore import generate_record_info

from rdflib import Namespace

FEDORA_M_NS = Namespace('http://www.fedora.info/definitions/1/0/management/')
MODS_NS = Namespace("http://www.loc.gov/mods/v3")

def __set_role__(role_term, object_id, output):
    """Helper function for setting a role

    Function takes a role term, ObjectId, and an output dictionary,
    creates or adds the role to a list in output withe serialized ID of the
    object.

    Args:
        role_term: String value of the role
        object_id: ObjectId
        output: Dictionary of values

    Returns:
        output: Dictionary of values with the role added
    """
    if role_term in output:
        output[role_term].append(str(object_id))
    else:
        output[role_term] = [str(object_id)]
    return output


def get_or_add_article(mods, client, record_constants):
    """Returns id of an existing Article or a new Article

    Args:
        mods: MODS etree
        client: Mongo DB Client
        record_constants: Dictionary of record constants

    Returns:
        ObjectId: Mongo DB ObjectId for the schema.org Article
    """
    schema_org = client.schema_org
    bibframe = client.bibframe


def get_or_add_audio(mods, client, record_constants):
    """Returns id of an existing AudioObject or a new AudioObject

    Args:
        mods: MODS etree
        client: Mongo DB Client
        record_constants: Dictionary of record constants

    Returns:
        ObjectId: Mongo DB ObjectId for the schema.org AudioObject
    """
    schema_org = client.schema_org
    bibframe = client.bibframe
    title = mods.find("{{{0}}}titleInfo/{{{0}}}title".format(MODS_NS))
    existing_audio = schema_org.CreativeWork.find_one(
        {"@type": 'AudioObject', 'headline': title.text},
        {'_id': 1})
    if existing_audio is not None:
        return existing_audio.get('_id')
    audio_object = AudioObject(**add_base(mods, client, record_constants))
    audio_object_id = schema_org.CreativeWork.ingest(audio_object.as_dict())
    audio = bf_models.Audio(
        label=title.text,
        relatedTo=[str(audio_object_id)])
    return audio_object_id


def get_or_add_map(mods, client, record_constants):
    schema_org = client.schema_org
    bibframe = client.bibframe
    title = mods.find("{{{0}}}titleInfo/{{{0}}}title".format(MODS_NS))
    existing_map = schema_org.CreativeWork.find_one(
        {"@type": "Map",
         "headline":title.text})
    if existing_map is not None:
        return existing_map.get('_id')
    base_mods = add_base(mods, client, record_constants)
    map_work = Map(**base_mods)
    map_dict = map_work.as_dict()
    map_dict['@type'] = 'Map'
    map_id = schema_org.CreativeWork.insert(map_dict)
    cartography = bf_models.Cartography(label=map_dict.get('name',
                                                map_dict.get('headline')),
                                        relatedTo=[str(map_id)])
    setattr(cartography,
            'recordInfo',
            generate_record_info(
                record_constants['source'],
                record_constants['msg']))
    cartography_id = bibframe.Work.insert(cartography.as_dict())
    schema_org.CreativeWork.update({"_id": map_id},
                                   {"$set": {'sameAs': [str(cartography_id)]}})

    return map_id



def get_or_add_organization(name, client, record_constants):
    """Existing organization or adds new Organization

    Args:
        name: Name string
        client: Mongo DB Client
        record_constants: Dictionary of record constants

    Returns:
        ObjectId: Mongo DB ObjectId for the schema.org Person
    """
    schema_org = client.schema_org
    bibframe = client.bibframe
    existing_org = schema_org.Organization.find_one({"name": name},
                                                    {"_id": 1})
    if existing_org:
        return existing_org.get("_id")
    organization = Organization(name=name)
    setattr(organization,
            'recordInfo',
            generate_record_info(
                record_constants.get('source'),
                record_constants.get('msg')))
    new_org = schema_org.Organization.insert(organization.as_dict())
    bf_organization = bf_models.Organization(
        relatedTo=[str(new_org),],
        label=name)
    setattr(bf_organization,
            'recordInfo',
            generate_record_info(
                record_constants.get('source'),
                record_constants.get('msg')))
    bf_id = bibframe.Organization.insert(bf_organization.as_dict())
    schema_org.Organization.update({"_id": new_org},
                                   {"$set": {"sameAs": [str(bf_id)]}})
    return new_org


def get_or_add_periodical(mods, client, record_constants):
    """Takes a MODS etree and gets ors adds a Periodical

    Periodical model uses the proposal at
    http://www.w3.org/community/schemabibex/ to add support for recurring
    resources in the MongoDatastore

    Args:
        mods: MODS XML etree
        client: Mongo DB Client
        record_constants: Dictionary of Record constants

    Returns:
        ObjectId: Mongo DB ObjectId for the schema.org Thesis
    """
    schema_org = client.schema_org
    bibframe = client.bibframe
    title = mods.find("{{{0}}}titleInfo/{{{0}}}title".format(MODS_NS))
    existing_periodical = schema_org.CreativeWork.find_one(
        {"@type": 'Periodical',
         "headline": title.text})
    if existing_periodical is not None:
        return existing_periodical.get('_id')
    base_mods = add_base(mods, client, record_constants)
    periodical = CreativeWork(**base_mods)
    periodical_dict = periodical.as_dict()
    periodical_dict['@type'] = 'Periodical'
    periodical_id = schema_org.CreativeWork.insert(periodical_dict)
    return periodical_id

def get_or_add_person(name, client, record_constants):
    """Function retrieves a schema:Person or adds a new schema:Person from MODS

    This function assumes that the mods name/namePart text is in the format of
    "familyName, givenName".

    Args:
        name: MODS name element
        client: Mongo DB Client
        record_constants: Dictionary with source and message for record info

    Returns:
        ObjectId: Mongo DB ObjectId for the schema.org Person
    """
    schema_org = client.schema_org
    bibframe = client.bibframe
    name_type = name.attrib.get('type')
    if not name_type.startswith('personal'):
        return
    bf_person = bf_models.Person(recordInfo=generate_record_info(
                                        record_constants.get('source'),
                                        record_constants.get('msg')))

    nameParts = name.findall("{{{0}}}namePart".format(MODS_NS))
    if len(nameParts) == 0:
        return
    elif len(nameParts) == 1:
        full_name = nameParts[0].text
        if full_name is None:
            return
        name_list = [part.strip() for part in full_name.split(",") if part.find('editor') < 0]
        if full_name.find('editor') > -1:
            full_name = ', '.join(name_list)
        if name_list[-1].find("."): # Removes middle initial
            name_list[-1] = name_list[-1].split(" ")[0]
        existing_person = schema_org.Person.find_one({"name": full_name},
                                                     {"_id": 1})
        if existing_person:
            return existing_person.get('_id')
    else:
        name_list = ['', '']
        for part in nameParts:
            if part.attrib.get('type') == 'family':
                name_list[0] = part.text
            if part.attrib.get('type') == 'given':
                name_list[1] = part.text
        full_name = ', '.join(name_list)
    person = Person(givenName=name_list[-1],
                    familyName=name_list[0],
                    name=full_name)
    person.recordInfo = generate_record_info(
                            record_constants.get('source'),
                            record_constants.get('msg'))
    person.sameAs = []
    person_id = schema_org.Person.insert(person.as_dict())
    bf_person.relatedTo = [str(person_id)]
    bf_person.label = person.name
    bf_person_id = bibframe.Person.insert(bf_person.as_dict())
    schema_org.Person.update({"_id": person_id},
                             {"$push": {'sameAs': str(bf_person_id)}})
    return person_id

def get_or_add_photograph(mods, client, record_constants):
    """Retrives or inserts MODS into MongoDB schema_org and bibframe databases

    Args:
        mods: MODS XML etree
        client: Mongo DB Client
        record_constants: Dictionary with constant values for record creation

    Returns:
        ObjectId: Mongo DB ObjectId for the schema.org Photograph
    """
    bibframe = client.bibframe
    schema_org = client.schema_org
    title = mods.find("{{{0}}}titleInfo/{{{0}}}title".format(MODS_NS))
    existing_photo = schema_org.CreativeWork.find_one(
        {"@type": 'Photograph', 'headline': title.text})
    if existing_photo is not None:
        return existing_photo.get('_id')
    photograph = Photograph(**add_base(mods, client, record_constants))
    photograph_dict = photograph.as_dict()
    photograph_id = schema_org.CreativeWork.insert(photograph_dict)
    still_image = bf_models.StillImage(
        label=title.text,
        relatedTo=[str(photograph_id)])
    still_image_id = bibframe.Work.insert(still_image.as_dict())
    instance = bf_models.Instance(instanceOf=str(still_image_id))
    schema_org.CreativeWork.update(
        {"_id": photograph_id},
        {"$set": {'sameAs': [str(still_image_id)]}})
    return photograph_id


def get_or_add_video(mods, client, record_constants):
    """Retrives or inserts MODS into MongoDB schema_org and bibframe databases

    Args:
        mods: MODS XML etree
        client: Mongo DB Client
        record_constants: Dictionary with constant values for record creation

    Returns:
        ObjectId: Mongo DB ObjectId for the schema.org VideoObject
    """
    bibframe = client.bibframe
    schema_org = client.schema_org
    title = mods.find("{{{0}}}titleInfo/{{{0}}}title".format(MODS_NS))
    existing_video = schema_org.CreativeWork.find_one(
        {"@type": 'VideoObject', 'headline': title.text})
    if existing_video is not None:
        return existing_video.get('_id')
    video_object = VideoObject(**add_base(mods, client, record_constants))
    video_object_dict = video_object.as_dict()
    video_id = schema_org.CreativeWork.insert(video_object_dict)
    moving_image = bf_models.MovingImage(
        **{'label':title.text,
           'relatedTo':[str(video_id)]})
    moving_image_id = bibframe.Work.insert(moving_image.as_dict())
    instance = bf_models.Instance(instanceOf=str(moving_image_id))
    schema_org.CreativeWork.update(
        {"_id": video_id},
        {"$set": {'sameAs': [str(moving_image_id)]}})
    return video_id


def add_base(mods, client, record_constants):
    """Adds common elements from MODS to their schema.org counterparts

    Args:
        mods: MODS XML etree
        client: Mongo DB Client
        record_constants: Dictionary with constant values for record creation

    Returns:
        dict: Dictionary of schema.org properties
    """
    schema_org = client.schema_org
    bibframe = client.bibframe
    instance = bf_models.Instance()
    output = generate_record_info(record_constants.get('source'),
                record_constants.get('msg'))
    # Process MODS name
    for name in mods.findall("{{{0}}}name".format(MODS_NS)):
        name_type = name.attrib.get('type', None)
        role = name.find("{{{0}}}role/{{{0}}}roleTerm".format(MODS_NS))
        if name_type == 'personal':
            person_id = get_or_add_person(name, client, record_constants)
            if person_id is not None and role is not None:
                if role.text == 'creator':
                    output = __set_role__('creator',
                                           person_id,
                                           output)
                if role.text == 'contributor':
                    output = __set_role__('contributor',
                                          person_id,
                                          output)

        elif name_type == 'corporate':
            corporate_name = name.find("{{{0}}}namePart".format(MODS_NS)).text
            if corporate_name is None or len(corporate_name) < 1:
                continue
            org_id = get_or_add_organization(corporate_name,
                        client,
                        record_constants)
            if role.text == 'contributor':
                output = __set_role__('contributor',
                                      org_id,
                                      output)
    # Process MODS title
    title = mods.find("{{{0}}}titleInfo/{{{0}}}title".format(MODS_NS))
    if title is not None:
        output['headline'] = title.text
    # Process MODS originInfo
    originInfo = mods.find("{{{0}}}originInfo".format(MODS_NS))
    publisher = originInfo.find("{{{0}}}publisher".format(MODS_NS))
    if publisher is not None and publisher.text:
        publisher_id = get_or_add_organization(publisher.text,
                                               client,
                                               record_constants)
        if publisher_id:
            output['publisher'] = str(publisher_id)
            output['copyrightHolder'] = [output['publisher'],]

    dateIssued = originInfo.find("{{{0}}}dateIssued".format(MODS_NS))
    if dateIssued is not None:
        output['datePublished'] = dateIssued.text
    dateCreated = originInfo.find("{{{0}}}dateCreated".format(MODS_NS))
    if dateCreated is not None:
        output['dateCreated'] = dateCreated.text
    # Process MODS subjects
    subjects = mods.findall("{{{0}}}subject".format(MODS_NS))
    if len(subjects) > 0:
        output['keywords'] = []
        for subject in subjects:
            first_element = subject.getchildren()[0]
            if len(first_element.getchildren()) > 0:
                grand_child = first_element.getchildren()[0]
                if grand_child.text is not None:
                    output['keywords'].append(grand_child.text)
            else:
                if first_element.text is not None:
                    output['keywords'].append(first_element.text)
    # Process MODS location
    location_url = mods.find("{{{0}}}location/{{{0}}}url".format(MODS_NS))
    if location_url is not None:
        output['url'] = location_url.text
    return output




def add_publication_issue(mods, client, issue_number, record_constants):
    schema_org = client.schema_org
    bibframe = client.bibframe
    base_mods = add_base(mods, client, record_constants)
    publication_issue = CreativeWork(**base_mods)
    setattr(publication_issue, 'issueNumber', issue_number)
    pub_issue_dict = publication_issue.as_dict()
    pub_issue_dict['@type'] = 'PublicationIssue'
    pub_issue_id = schema_org.CreativeWork.insert(pub_issue_dict)

    return pub_issue_id

def add_publication_volume(mods, client, volume, record_constants):
    schema_org = client.schema_org
    bibframe = client.bibframe
    base_mods = add_base(mods, client, record_constants)
    publication_volume = CreativeWork(**base_mods)
    setattr(publication_volume, 'volumeNumber', volume)
    pub_volume_dict = publication_volume.as_dict()
    pub_volume_dict['@type'] = 'PublicationVolume'
    pub_volume_id = schema_org.CreativeWork.insert(pub_volume_dict)
    return pub_volume_id



def add_thesis(mods, client, ):
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
    return thesis.save()































