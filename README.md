# Semantic Server
This project is part of the [Catalog Pull Platform](http://intro2libsys.info/catalog-pull-platform)
and provides a REST API using  
[Falcon][FALCON], [Fedora Commons][FEDORA], [Elastic Search][ES],  
and [Fuseki][FUSEKI] for managing Linked Data
artifacts by memory and cultural heritage institutions. 
.

## Components

*   [Fedora Commons Digital Repository][FEDORA] as Linked Data platform.
*   [Elasticsearch][ES] for searching
*   [Fuseki][FUSEKI] as SPARQL endpoint
*   [Redis][REDIS] for result caching and analytics

[ES]: http://www.elasticsearch.org/
[FALCON]: http://falconframework.org/
[FEDORA]: http://fedora-commons.org
[FUSEKI]: http://jena.apache.org/documentation/serving_data/
[REDIS]: http://redis.io
