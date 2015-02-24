"""This module provides a REST interface to an Islandora REST end-point

"""
__author__ = "Jeremy Nelson"

import falcon
import requests
##import urllib.request
##from base64 import encodestring

class IslandoraBase(object):

    def __init__(self, config):
        if not 'ISLANDORA' in config:
            raise falcon.HTTPInternalServerError(
            "ISLANDORA config missing",
            "ISLANDORA section in config.py is missing")
        self.islandora = config['ISLANDORA']
        if not "port" in self.islandora:
            self.base_url = "http://{}".format(self.islandora["host"])
        else:
            self.base_url = "http://{}:{}".format(
                self.islandora["host"],
                self.islandora["port"])
        self.auth = None
        if "username" in self.islandora and "password" in self.islandora:
            self.auth = (self.islandora["username"], self.islandora["password"])

class IslandoraDatastream(IslandoraBase):

    def __init__(self, config, pid=None):
        super(IslandoraDatastream, self).__init__(config)
        if pid:
            self.rest_url = "{}/rest/v1/object/{}/datastreams".format(
                self.base_url,
                pid)
        else:
            self.rest_url = "{}/rest/v1/object/".format(self.base_url)

    def __add__(self, data, stream, pid=None):
        if not self.rest_url.endswith("datastreams"):
            if not pid:
                raise falcon.HTTPMissingParam(
                    "Cannot add a datastream - missing pid",
                    "pid")
            self.rest_url += "{}/datastreams".format(pid)
        add_ds_req = requests.post(
                self.rest_url,
                data=data,
                files=files, auth=self.auth)
        if add_ds_req.status_code > 399:
            raise falcon.HTTPInternalServerError(
                "Failed to add datastream to Islandora",
                "Islandora Status Code {}".format(add_ds_req.status_code))
        return True

    def on_post(self, req, resp, stream=None):



class IslandoraObject(IslandoraBase):

    def __init__(self, config):
        super(IslandoraObject, self).__init__(config)



    def __add_datastream__(self, pid, data, stream, label='Primary File'):

        if add_ds_req.status_code > 399:
            raise falcon.HTTP





    def __add_relationship__(self, uri, predicate, object_):
        return


    def on_post(self, req, resp, pid=None):
        mods = req.get_param('mods') or None
        primary_file = req.get_param('file') or None
        content_model = req.get_param('content_model') or None
        parent_pid = req.get_param('parent_pid') or 'islandora:root'
        data = {}
        if "namespace" in self.islandora:
            data['namespace'] = self.islandora["namespace"]
        if pid:
            url = "{}/rest/v1/object/{}".format(self.base_url, pid)
            data['pid'] = pid
        else:
            url = "{}/rest/v1/object".format(self.base_url)

        add_object_req = requests.post(url, data=data, auth=self.auth)
        if not pid:
            pid = add_object_req.json()['pid']
        islandora_relationship = IslandoraRelationship(config, pid)
        # Add Content Model relationship
        islandora_relationship.__add__(
            "info:fedora/fedora-system:def/model#",
            "hasModel",
            content_model)
        # Add to Parent Collection
        islandora_relationship.__add__(
                "info:fedora/fedora-system:def/relations-external#",
                "isMemberOfCollection",
                parent_pid)
        islandora_datastream = IslandoraDatastream(config, pid)
        # Add MODS metadata if present
        if mods:
            islandora_datastream.__add__(
                {"dsid": "MODS", "label": "MODS"},
                {"file": mods})
        # Adds Primary File
        if primary_file:
            data = {
                'dsid': req.get_param("file_disd") or "PRIMARY_DATASTREAM",
                'label': req.get_param("file_label") or\
             "Primary datastream for Object {}".format(pid)}
            islandora_datastream.__add__(data, {"file": primary_file})
        return json.dumps({"message": "Successfully added {}".format(pid)})



class IslandoraRelationship(IslandoraBase):

    def __init__(self, config, pid=None):
        super(IslandoraRelationship, self).__init__(config)
        self.pid = pid
        if self.pid:
            self.rest_url = "{}/rest/v1/object/{}/relationship".format(
                self.base_url,
                self.pid)
        else:
            self.rest_url = "{}/rest/v1/object/".format(self.base_url)



    def __add__(self, namespace, predicate, object_, pid=None):
        self.__set_rest_url__(pid)
        data = {
            "predicate": predicate,
            "object": object_,
            "uri": namespace,
            "literal": "false",
            "type": "nil"}
        add_relationship_req = requests.post(
            self.rest_url,
            data,
            auth=self.auth)
        if add_relationship_req.status_code > 399:
            raise falcon.HTTPInternalServerError(
                "Failed to add relationship",
                "Failed to add relationship with url {}".format(self.rest_url))
        return True

    def __set_rest_url__(self, pid=None):
        if not self.pid and not pid:
            raise falcon.HTTPMissingParam(
                "Cannot add relationship - missing pid",
                "pid")
        elif not pid:
            pid = self.pid
        if not self.rest_url.endswith("relationship"):
            self.rest_url +="{}/{}/relationship".format(self.rest_url, pid)

    def on_get(self, req, resp, pid=None):
        self.__set_rest_url__(pid)



    def on_post(self, req, resp, pid=None):
        self.__set_rest_url__(pid)
        namespace = req.get_param("namespace") or\
            "info:fedora/fedora-system:def/relations-external#"
        predicate = req.get_param("predicate") or None
        if not predicate:
            raise falcon.HTTPMissingParam(
                "Islandora Relationship POST missing predicate param",
                "predicate")
        object_ = req.get_param("object") or None
        if not object_:
            raise falcon.HTTPMissingParam(
                "Islandora POST missing object param")
        if self.__add__(namespace, predicate, object_):
            message = """Added relationship {} from {} to {}""".format(
                predicate,
                pid,
                object_)
            return json.dumps(
                {"message": message})
        else:
            return json.dumps(
                {"message": "failed to add relationship to {}".format(pid)})













