"""This module provides a REST interface to an Islandora REST end-point

This module's classes are Falcon-based REST api endpoints to an
Islandora/Fedora Drupal front-end. The Islandora instance needs to have the
following Islandora and Drupal modules installed to work correctly with this
module.

## Islandora
https://github.com/discoverygarden/islandora_rest

## Drupal Module
Secure Site - https://www.drupal.org/project/securesite


"""
__author__ = "Jeremy Nelson"

import cgi
import falcon
import io
import json
import requests

class IslandoraBase(object):

    def __init__(self, config):
        if not 'ISLANDORA' in config:
            raise falcon.HTTPInternalServerError(
            "ISLANDORA config missing",
            "ISLANDORA section in config.py is missing")
        self.islandora = config['ISLANDORA']
        if "port" in self.islandora:
            self.base_url = "http://{}:{}".format(
                self.islandora["host"],
                self.islandora["port"])
        else:
            self.base_url = "http://{}".format(self.islandora["host"])
        self.rest_url = "{}/islandora/rest/v1/object".format(self.base_url)
        self.auth = None
        if "username" in self.islandora and "password" in self.islandora:
            self.auth = (self.islandora["username"], self.islandora["password"])

    def __set_rest_url__(self, label, pid=None):
        if not self.pid and not pid:
            raise falcon.HTTPMissingParam(
                "Cannot add {} - missing pid".format(label),
                "pid")
        if not pid:
            pid = self.pid
        if self.rest_url.endswith("/"):
            self.rest_url = self.rest_url[:-1]
        if label:
            if not self.rest_url.endswith(label):

                self.rest_url ="{}/{}/{}".format(self.rest_url, pid, label)
        else:
            self.rest_url = "{}/{}".format(self.rest_url, pid)


class IslandoraDatastream(IslandoraBase):

    def __init__(self, config, pid=None):
        super(IslandoraDatastream, self).__init__(config)
        self.pid = pid
        if pid:
            self.pid = pid
            self.rest_url = "{}/islandora/rest/v1/object/{}/datastreams".format(
                self.base_url,
                pid)
        else:
            self.pid = None
            self.rest_url = "{}/islandora/rest/v1/object/".format(self.base_url)


    def __add__(self, data, stream, pid, dsid):
        """Internal method takes data dictionary and a bitstream along with
        a pid and adds the datastream to the Islandora Object at the given
        pid.

        Args:
            data -- Dictionary of properties about the datastream
            stream -- bitstream
            pid -- PID to add
            dsid -- Datastream ID, if None will raise Error

        Returns
            boolean or raise falcon.HTTPMissingParameter
        """
        if not dsid:
            raise falcon.HTTPMissingParam(
                "dsid")
        rest_url = "{}/islandora/rest/v1/object/{}/datastream".format(
            self.base_url,
            pid)
        if not 'namespace' in data:
            data['namespace'] = self.islandora.get('namespace', "islandora")
        add_ds_req = requests.post(
                rest_url,
                data=data,
                files={"file": stream},
                auth=self.auth)
        if add_ds_req.status_code > 399:
            raise falcon.HTTPInternalServerError(
                "Failed to add datastream to Islandora",
                """Islandora Status Code {}
                Datastream id={} Object pid={}""".format(
                    add_ds_req.status_code,
                    data.get('disd', None),
                    pid))
        return True

    def on_get(self, req, resp, pid, dsid=None):
        if dsid is None:
            raise falcon.HTTPMissingParam(
                "Cannot retrieve datastream - missing dsid",
                "dsid")
        rest_url =  "{}/islandora/rest/v1/object/{}/datastreams".format(
            self.base_url,
            pid,
            dsid)
        get_ds_req = requests.get(rest_url)
        if get_ds_req.status_code > 399:
            raise falcon.HTTPInternalServerError(
                "Failed to get datastream from Islandora",
                "Get URL={}, Islandora Status Code={}".format(
                    get_url,
                    get_ds_req.status_code))
        resp.status = falcon.HTTP_200
        resp.body = json.dumps(get_ds_req.json())

    def on_post(self, req, resp, pid, dsid=None):
        env = req.env
        env.setdefault('QUERY_STRING', '')
        form = cgi.FieldStorage(fp=req.stream, environ=env)
        file_item = form['userfile']
        data = {"dsid": dsid or "FILE_UPLOAD"}
        if "state" in form:
            data["state"] = form["state"].value
        else:
            data["state"] = "A"
        if "control_group" in form:
            data["controlGroup"] = form['control_group'].value
        else:
             data["controlGroup"] = "M"
        if "label" in form:
            data["label"] = form["label"].value
        else:
            data["label"] = file_item.name
        if "mime_type" in form:
            data["mimeType"] = form["mime_type"].value
        else:
            data["mimeType"] = 'application/octet-stream'
        if self.__add__(data, file_item.file, pid, dsid):
            resp.status = falcon.HTTP_201
            resp.body = json.dumps({
                "message": "Added {} datastream to {}".format(dsid, pid)})
        else:
            desc = "{} datastream was not added object".format(dsid)
            desc += "{}\nusing POST URL={}".format(pid, self.rest_url)
            raise falcon.HTTPInternalServerError(
                "Failed to add datastream to object",
                desc)




class IslandoraObject(IslandoraBase):

    def __init__(self, config, pid=None):
        super(IslandoraObject, self).__init__(config)
        self.pid = pid
        if self.pid:
            self.rest_url = "{}/islandora/rest/v1/object/{}".format(
                self.base_url,
                pid)
        else:
            self.rest_url = "{}/islandora/rest/v1/object".format(self.base_url)

    def __add_stub__(self, label, namespace, pid=None):
        """Internal method takes label, namespace, and an optional pid
        creates a new Islandora Object and returns JSON result

        Args:
            label -- Label for new Islandora Object
            namespace -- Namespace for repository
            pid -- Optional pid
        """
        data = {"label": label, "namespace": namespace} 
        if pid:
            data['pid'] = pid
        add_url = "{}/islandora/rest/v1/object".format(self.base_url)
        add_object_result = requests.post(add_url, data=data, auth=self.auth)
        if add_object_result.status_code > 399:
            raise falcon.HTTPInternalServerError(
                "Failed to add Islandora object",
                "Failed with url={}, islandora status code={}\n{}".format(
                    add_url,
                    add_object_result.status_code,
                    add_object_result.text))
        return add_object_result.json()
        

    def on_get(self, req, resp, pid=None):
        self.__set_rest_url__(None, pid)
        get_obj_req = requests.get(self.rest_url)
        if get_obj_req.status_code > 399:
            raise falcon.HTTPInternalServerError(
                "Failed to retrieve object with from Islandora",
                "Failed with url={}, islandora status code={}".format(
                    self.rest_url, get_obj_req.status_code))

        resp.status = falcon.HTTP_200
        resp.body = json.dumps(get_obj_req.json())

    def on_post(self, req, resp, pid=None):
        if pid:
            self.__set_rest_url__(None, pid)
        msg = {'datastreams':[]}
        primary_file = req.get_param('file') or None
        content_model = req.get_param('content_model')
        label = req.get_param('label') or "LABEL for {}".format(pid)
        namespace = req.get_param('namespace') or self.islandora.get(
                                                      'namespace', 
                                                      "islandora")
        add_object_msg = self.__add_stub__(label, namespace, pid) 
        parent_pid = req.get_param('parent_pid') or 'islandora:root'
        if not pid:
            pid = add_object_msg['pid']
            data['pid'] = pid
        msg['pid'] = pid
        islandora_relationship = IslandoraRelationship(
            {"ISLANDORA": self.islandora},
            pid)
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
        islandora_datastream = IslandoraDatastream(
            {"ISLANDORA": self.islandora},
            pid)
        # Adds Primary File
        if primary_file:
            data = {
                'dsid': req.get_param("file_disd") or "PRIMARY_DATASTREAM",
                'label': req.get_param("file_label") or\
             "Primary datastream for Object {}".format(pid)}
            islandora_datastream.__add__(data, {"file": primary_file})
            msg['datastreams'].append(data['dsid'])
        msg['message'] = "Successfully ingest {} into Islandora".format(pid)
        resp.status = falcon.HTTP_201
        resp.body = json.dumps(msg)


class IslandoraRelationship(IslandoraBase):

    def __init__(self, config, pid=None):
        super(IslandoraRelationship, self).__init__(config)
        self.pid = pid
        if self.pid:
            self.rest_url = "{}/islandora/rest/v1/object/{}/relationship".format(
                self.base_url,
                self.pid)
        else:
            self.rest_url = "{}/islandora/rest/v1/object/".format(self.base_url)


    def __add__(self, namespace, predicate, object_, pid=None):
        self.__set_rest_url__("relationship", pid)
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


    def on_get(self, req, resp, pid=None):
        self.__set_rest_url__("relationship", pid)
        resp.status = falcon.HTTP_200
        resp.body = json.dumps()



    def on_post(self, req, resp, pid=None):
        self.__set_rest_url__("datastream", pid)
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
                "Islandora POST missing object param",
                "object")
        if self.__add__(namespace, predicate, object_):
            message = """Added relationship {} from {} to {}""".format(
                predicate,
                pid,
                object_)
            resp.status = falcon.HTTP_201
            resp.body = json.dumps({"message": message})
        else:
            raise
            resp.status = falcon.HTTP_400
            resp.body = json.dumps(
                {"message": "failed to add relationship to {}".format(pid)})













