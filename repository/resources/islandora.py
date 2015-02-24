"""This module provides a REST interface to an Islandora REST end-point

"""
__author__ = "Jeremy Nelson"

import falcon
import requests
##import urllib.request
##from base64 import encodestring

class IslandoraObject(object):

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

##        if "username" in self.islandora and "password" in self.islandora:
##            encoded_usr_pwd = encodestring(
##                "{}:{}".format(
##                    self.islandora["username"],
##                    self.islandora["password"]).encode())[:-1]
##            self.auth = "Basic {}".format(encoded_usr_pwd)

    def __add_relationship__(self, uri, predicate, object_):
        return {
            "predicate": predicate,
            "object": object_,
            "uri": uri,
            "literal": "false",
            "type": "nil"
        }


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
        # Add Content Model
        add_content_type = requests.post(
            "{}/{}/relationship".format(self.base_url, pid),
            data=self.__add_relationship__(
                "info:fedora/fedora-system:def/model#",
                "hasModel",
                content_model),
            auth=self.auth)
        # Add to Parent Collection
        add_parent_collection = requests.post(
            "{}/{}/relationship".format(self.base_url, pid),
            data=self.__add_relationship__(
                "info:fedora/fedora-system:def/relations-external#",
                "isMemberOfCollection",
                parent_pid),
            auth=self.auth)
        # Add MODS metadata if present
        if mods:
            url = "{}/{}/datastreams".format(url, pid)
            data = {"dsid": "MODS", "label": "MODS"}
            files = {"file": mods}
            add_mods_req = requests.post(
                url,
                data=data,
                files=files, auth=self.auth)
        # Adds Primary File
        if primary_file:
            url = "{}/{}/datastreams".format(url, pid)
















