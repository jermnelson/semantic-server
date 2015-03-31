__author__ = "Jeremy Nelson"

from .ingesters import GraphIngester

class Ingester(GraphIngester):

    def __init__(self, **kwargs):
        super(Ingester, self).__init__(**kwargs)
        self.fusuki_ds = "schema"
    
    def __process_subject__(self, row):
        subject, graph = row
