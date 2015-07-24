__author__ = "Jeremy Nelson"


import rdflib
from .ingesters import GraphIngester

class Ingester(GraphIngester):

    def __init__(self, **kwargs):
        super(Ingester, self).__init__(**kwargs)

    def __process_subject__(self, row):
        subject, graph = row[0], row[1]
        schema_type = graph.value(subject=subject, predicate=rdflib.RDF.type)
        existing_uri = self.searcher.triplestore.__sameAs__(str(subject))
        if existing_uri:
            subject = rdflib.URIRef(existing_uri)
        fedora_url, new_graph = self.__add_or_get_graph__(
            subject=subject, 
            graph=graph,
            graph_type=schema_type)
        subject_uri = rdflib.URIRef(fedora_url)
        return subject_uri
