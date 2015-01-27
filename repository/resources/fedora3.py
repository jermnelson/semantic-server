__author__ = "Jeremy Nelson"
import falcon

from .. import Repository

ISLANDORA_CONTENT_MODELS = {
    "pdf": "islandora:sp_pdf",
    "mods": "http://www.loc.gov/mods/v3",
    "tif": "islandora:sp_large_image_cmodel",
    "wav": "islandora:sp_large_image_cmodel"
}

NAMESPACES ={
    "audit":"info:fedora/fedora-system:def/audit#",
    "dc":"http://purl.org/dc/elements/1.1/",
    "foxml": "info:fedora/fedora-system:def/foxml#",
    "oai_dc":"http://www.openarchives.org/OAI/2.0/oai_dc/"
}

class FedoraObject(Repository):

    def on_get(self, req, resp, pid):
        output = {"message": "Should Display Fedora Object {}".format(pid),
                  "name": req.get_param('name')}
        resp.body = str(output)
        resp.status = falcon.HTTP_200

    def on_post(self, req, resp, pid):
        name = req.get_param('name')
        resp.body = "name is {}".format(req.stream.read(4096))
        resp.status = falcon.HTTP_200

    def migrate_to(self, target_repository):
        """Method migrates Object to a target repository

        Args:
            target_repository -- Fedora Repository 3.7 or 3.8 repository
        """
        pass



