__author__ = "Jeremy Nelson"

class BaseSearch(object):
    """Search Repository"""

    def __init__(self, config):
        self.search_index = None
        if 'ELASTICSEARCH' in config:
            es_path = "{}:{}".format(
                config.get('ELASTICSEARCH', 'host'),
                config.get('ELASTICSEARCH', 'port'))
            if 'path' in config['ELASTICSEARCH']:
                es_path += "/{}".format(config.get('ELASTICSEARCH', 'path'))
            self.search_index = Elasticsearch(es_path)
        else: # Defaults to localhost 9200
            self.search_index = Elasticsearch()

    def __get_id_or_value__(self, value):
        """Helper function takes a dict with either a value or id and returns
        the dict value

        Args:
	    value(dict)
        Returns:
	    string or None
        """
        if [str, float, int, bool].count(type(value)) > 0:
            return value 
        elif '@value' in value:
            return value.get('@value')
        elif '@id' in value:
            result = self.triplestore.__get_id__(value.get('@id'))
            if len(result) > 0:
                if type(result) == str:
                    return result
                return result[0]['uuid']['value']
            return value.get('@id')
        return value

    def __generate_body__(self, graph):
        """Internal method generates the body for indexing into Elastic search
        based on the JSON-LD serializations of the Fedora Commons Resource graph.

        Args:
            graph -- rdflib.Graph of Resource
            prefix -- Prefix filter, will only index if object starts with a prefix,
                      default is None to index everything.
        """
        pass


    def __index__(self, subject, graph, doc_type, index): 
        self.__generate_body__(graph)
        doc_id = str(subject).split("/")[-1]
        self.__generate_suggestion__(subject, graph, doc_id)
        self.search_index.index(
            index=index,
            doc_type=doc_type,
            id=doc_id,
            body=self.body)

    def __set_or_expand__(self, key, value):
        """Helper method takes a key and value and either creates a key
        with either a list or appends an existing key-value to the value

        Args:
            key
            value
        """
        if key not in self.body:
           self.body[key] = []
        if type(value) == list:
            for row in value:
                self.body[key].append(self.__get_id_or_value__(row))
        else:
            self.body[key].append(self.__get_id_or_value__(value))

            
class GraphIndexer(BaseSearch):
    """Graph indexer takes a RDF Graph runs three different SPARQL queries
    to generate the Identifiers, Display, and Reference sections for the 
    Elasticsearch index."""

    def __init__(self, **kwargs):
        config = kwargs.get('config', dict())
        super(Search, self).__init__(config)
        self.id_sparql = kwargs.get('id_sparql')
        self.display_sparql = kwargs.get('display_sparql')
        self.reference_sparql = kwargs.get('reference_sparql')

    def __generate_body__(self, graph):
        body = dict()
        body['Identifiers'] = self.__add_predicate_object__(
            self.id_sparql, graph)
        body['Display'] = self.__add_predicate_object__(
            self.id_sparql, graph)
        body['reference'] = self.__add_predicate_object__(
            self.id_sparql, graph)
       return body

    def __add_predicate_object__(self, sparql, graph):
        output = dict()
        if not sparql:
            return output
        for row in graph.query(self.id_sparql):
            output[row['p']] = row['o']
        return output
      
        

        
class LegacySearch(BaseSearch):


    def __init__(self, config):
        super(Search, self).__init__(config)
        self.triplestore = TripleStore(config)
        self.body = None


    def __generate_body__(self, graph, prefix=None):
        """Internal method generates the body for indexing into Elastic search
        based on the JSON-LD serializations of the Fedora Commons Resource graph.

        Args:
            graph -- rdflib.Graph of Resource
            prefix -- Prefix filter, will only index if object starts with a prefix,
                      default is None to index everything.
        """
        self.body = dict()
        graph_json = json.loads(
            graph.serialize(
                format='json-ld',
                context=CONTEXT).decode())
        if '@graph' in graph_json:
            for graph in graph_json.get('@graph'):
                # Index only those graphs that have been created in the
                # repository
                if 'fedora:created' in graph:
                    for key, val in graph.items():
                        if key in [
                            'fedora:lastModified',
                            'fedora:created',
                            'fedora:uuid'
                        ]:
                            self.__set_or_expand__(key, val)
                        elif key.startswith('@type'):
                            for name in val:
                                #! prefix should be a list 
                                if prefix:
                                    if name.startswith(prefix):
                                        self.__set_or_expand__('type', name)
                                else:
                                    self.__set_or_expand__('type', name)
                        elif key.startswith('@id'):
                            self.__set_or_expand__('fedora:hasLocation', val)
                        elif not key.startswith('fedora') and not key.startswith('owl'):
                            self.__set_or_expand__(key, val) 
    def __update__(self, **kwargs):
        """Helper method updates a stored document in Elastic Search and Fuseki. 
        Method must have doc_id 

        Keyword args:
            doc_id -- Elastic search document ID
            field -- Field name to update index, raises exception if None
            value -- Field value to update index, raises exception if None
        """
        doc_id, doc_type, index = kwargs.get('doc_id'), None, None
        if not doc_id:
            raise falcon.HTTPMissingParam("doc_id")
        field = kwargs.get('field')
        if not field:
            raise falcon.HTTPMissingParam("field")
        if result.status_code < 400:
            bindings = result.json().get('results').get('bindings')
            if len(bindings) > 0:
                return bindings[0]['subject']['value']
        else:
            raise falcon.HTTPInternalServerError(
                "Failed to match query in Fuseki",
                "Predicate={} Object={} Type={}\nError:\n{}".format(
                    predicate,
                    object_,
                    type_,
                    result.text))
        value = kwargs.get('value')
        if not value:
            raise falcon.HTTPMissingParam("value")
        if self.triplestore:
            result = self.triplestore.__get_subject__(uuid=doc_id)
            self.triplestore.__update_triple__(
                str(result),
                field,
                value)
        if not self.search_index:
            return
        for row in self.search_index.indices.stats()['indices'].keys():
            # Doc id should be unique across all indices 
            if self.search_index.exists(index=row, id=doc_id): 
                result = self.search_index.get(index=row, id=doc_id)
                doc_type = result['_type']
                index=row
                break
        if doc_type is None or index is None:
            raise falcon.HTTPNotFound()                 
        self.search_index.update(
            index=index,
            doc_type=doc_type,
            id=doc_id,
            body={"doc": {
                field: self.__get_id_or_value__(value)
            }})

    def on_patch(self, req, resp):
        """Method takes either sparql statement or predicate and object 
        and updates the Resource.

        Args:
            req -- Request
            resp -- Response
        """
        doc_uuid = req.get_param('uuid')
        if not doc_uuid:
            raise falcon.HTTPMissingParam('uuid')
        predicate = req.get_param('predicate') or None
        if not predicate:
            raise falcon.HTTPMissingParam('predicate')
        object_ = req.get_param('object') or None
        if not object_:
            raise falcon.HTTPMissingParam('object')
        doc_type = req.get_param('doc_type') or None
        if self.__update__(
            doc_id=doc_uuid,
            doc_type=doc_type,
            field=predicate,
            value=object_):
            resp.status = falcon.HTTP_202
            resp.body = json.dumps(True)
        else:
            raise falcon.HTTPInternalServerError(
                "Error with PATCH for {}".format(doc_uuid),
                "Failed setting {} to {}".format(
                    predicate,
                    object_))

    def on_get(self, req, resp):
        """Method takes a a phrase, returns the expanded result.

        Args:
            req -- Request
            resp -- Response
        """
        phrase = req.get_param('phrase') or '*'
        size = req.get_param('size') or 25
        resource_type = req.get_param('resource') or None
        if resource_type:
            resp.body = json.dumps(self.search_index.search(
                q=phrase,
                doc_type=resource_type,
                size=size))
        else:
            resp.body = json.dumps(self.search_index.search(
                q=phrase,
                size=size))
        resp.status = falcon.HTTP_200

