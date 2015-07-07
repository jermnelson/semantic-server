__author__ = "Jeremy Nelson"

import re
from .namespaces import *

PREFIX = """PREFIX fedora: <{}>
PREFIX owl: <{}>
PREFIX rdf: <{}>
PREFIX xsd: <{}>""".format(FEDORA, OWL, RDF, XSD)


DEDUP_SPARQL = """{}
SELECT ?subject
WHERE {{{{
    ?subject <{{}}> "{{}}"^^xsd:string .
    ?subject rdf:type <{{}}> .
}}}}""".format(PREFIX)

GET_ID_SPARQL = """{}
SELECT ?uuid
WHERE {{{{
 <{{}}> fedora:uuid ?uuid .
}}}}""".format(PREFIX)

GET_SUBJECT_SPARQL = """{}
SELECT DISTINCT *
WHERE {{{{
 ?subject {{}} {{}} .
}}}}""".format(PREFIX)


LOCAL_SUBJECT_PREDICATES_SPARQL = """{}
SELECT DISTINCT *
WHERE {{{{
  ?subject ?predicate <{{0}}> .
  FILTER NOT EXISTS {{{{ ?subject owl:sameAs <{{0}}> }}}}

}}}}""".format(PREFIX)

REPLACE_OBJECT_SPARQL = """{}
DELETE {{{{
    <{{0}}> {{1}} {{2}} .
}}}}
INSERT {{{{
    <{{0}}> {{1}} {{3}} 
}}}}
WHERE {{{{
}}}}""".format(PREFIX)



SAME_AS_SPARQL = """{}
SELECT DISTINCT ?subject
WHERE {{{{
  ?subject owl:sameAs {{}} .
}}}}""".format(PREFIX)


UPDATE_TRIPLESTORE_SPARQL = """{}
INSERT DATA {{{{
   {{}}
}}}}""".format(PREFIX)

PREFIX_CHECK_RE = re.compile(r'\w+[:][a-zA-Z]')


URL_CHECK_RE = re.compile(
    r'^(?:http|ftp)s?://' # http:// or https://
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' # domain...
    r'localhost|' # localhost...
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|' # ...or ipv4
    r'\[?[A-F0-9]*:[A-F0-9:]+\]?)' # ...or ipv6
    r'(?::\d+)?' # optional port
    r'(?:/?|[/?]\S+)$', re.IGNORECASE)

