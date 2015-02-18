__author__ = "Jeremy Nelson"

import falcon
import urllib.request
from .. import Repository

ISLANDORA_CONTENT_MODELS = {
    "pdf": "islandora:sp_pdf",
    "tif": "islandora:sp_large_image_cmodel",
    "wav": "islandora:sp_large_image_cmodel"
}

NAMESPACES ={
    "audit":"info:fedora/fedora-system:def/audit#",
    "dc":"http://purl.org/dc/elements/1.1/",
    "foxml": "info:fedora/fedora-system:def/foxml#",
    "mods": "http://www.loc.gov/mods/v3",
    "oai_dc":"http://www.openarchives.org/OAI/2.0/oai_dc/"
}

class FedoraObject(Repository):

    def __init__(self, config):
        super(FedoraObject, self).__init__(config)
        if not 'FEDORA3' in config:
            raise ValueError("FedoraObject requires Fedora 3.x configuration")
        self.base_url = "{}:{}/objects/".format(
            config['FEDORA3']['host'],
            config['FEDORA3']['port'])

    def on_get(self, req, resp, pid):
        # Uses Fedora Commons REST API-A
        method = req.get_param('method') or 'getObjectProfile'


        output = {"message": "Should Display Fedora Object {}".format(pid),
                  "name": req.get_param('name'),
                  "method": method}
        resp.body = str(output)
        resp.status = falcon.HTTP_200

    def on_post(self, req, resp, pid=None):
        mods = req.get_param('mods')
        name = req.get_param('name')
        if pid is None:
            ingest_url ="{}/new".format(self.base_url)
        else:
            ingest_url = "{}/{}".format(
                self.base_url,
                pid)

        resp.body = "name is {}".format(req.stream.read(4096))
        resp.status = falcon.HTTP_200

    def migrate_to(self, target_repository):
        """Method migrates Object to a target repository

        Args:
            target_repository -- Fedora Repository 3.7 or 3.8 repository
        """
        pass



