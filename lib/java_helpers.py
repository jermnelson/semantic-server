#-------------------------------------------------------------------------------
# Name:        java_helpers
# Purpose:     This module creates a socket server using jython for transforming
#              MARC xml to BIBFRAME RDF using the Saxon jquery engine and the
#              XQuery script from https://github.com/lcnetdev/marc2bibframe/
#
# Author:      Jeremy Nelson
#
# Created:     2014/05/08
# Copyright:   (c) Jeremy Nelson, Colorado College 2014
# Licence:     MIT
#-------------------------------------------------------------------------------
import datetime
import json
import os
import SocketServer
import sys

for filename in os.listdir("."):
    if os.path.splitext(filename)[-1].endswith(".jar"):
        sys.path.append(filename)

from java.io import File, FileInputStream, FileOutputStream
from java.lang import System
from java.net import URI
from javax.xml.transform.stream import StreamResult
from javax.xml.transform.sax import SAXSource
from net.sf import saxon
from org.xml.sax import InputSource

PROCESSOR = None

class XQueryProcessor(object):

    def __init__(self, **kwargs):

##        with open(kwargs.get('saxon_xqy')) as saxon_xqy:
##            query_stream = saxon_xqy.read()
##        self.processor = saxon.s9api.Processor(False)
##        xq_complier = self.processor.newXQueryCompiler()
##        xq_complier.setBaseURI(URI('file:///c:/Users/jernelson/Development/marc2bibframe/xbin/saxon.xqy'))
##        xquery_exp = xq_complier.compile(query_stream)
##        self.xquery_evaluator = xquery_exp.load()




##        self.config = saxon.Configuration.newConfiguration()
##        self.static_env = self.config.newStaticQueryContext()
##        self.static_env.setBaseURI(kwargs.get('base_uri', 'http://catalog/'))
##        print("Base URI={}".format(kwargs.get('base_uri')))
##        query_stream = FileInputStream(kwargs.get('saxon_xqy'))
##        self.xquery_exp = self.static_env.compileQuery(query_stream, None)

    def run(self, marc_xml):
        print("Before xquery evaluator")
##        self.xquery_evaluator.setExternalVariable(
##            saxon.s9api.QName('marcxmluri'),
##            saxon.s9api.XdmAtomicValue(marc_xml))
        #source = SAXSource(InputSource(marc_xml))
        #source.setSystemId('http://catalog/')
        self.xquery_evaluator.setExternalVariable(
            saxon.s9api.QName("marcxmluri"),
            saxon.s9api.XdmAtomicValue(marc_xml))
        out = self.processor.newSerializer(System.out)
        self.xquery_evaluator.run(out)
        print("After result={}".format(result))

##        dynamic_env = saxon.query.DynamicQueryContext(self.config)
##        params = saxon.expr.instruct.GlobalParameterSet()
##        params.put(
##            saxon.om.StructuredQName('', '', 'marcxmluri'),
##            marc_xml)
##        dynamic_env.setParameters(params)
##        result = self.xquery_exp.run(dynamic_env)
        return result


class Marc2BibframeTCPHandler(SocketServer.StreamRequestHandler):


    def handle(self):
##        config = saxon.Configuration.newConfiguration()
##        dynamic_env = saxon.query.DynamicQueryContext(config)
##        params = saxon.expr.instruct.GlobalParameterSet()
##        params.put(saxon.om.StructuredQName('', '', 'marcxmluri'),
##                   xml_location)
##        dynamic_env.setParameters(params)
        self.data = self.rfile.readline().strip()
        rdf_output = PROCESSOR.run(self.data)
        print("{} wrote XML".format(self.client_address[0]))
        self.wfile.write(rdf_output)

def marc2bibframe(**kwargs):
    xml_location = kwargs.get('marc_xml')
    base_uri = kwargs.get('base_uri', 'http://catalog')
    saxon_xqy = kwargs.get('saxon_xqy')
    rdf_location = "{}.rdf".format(xml_location)
    config = saxon.Configuration.newConfiguration()
    params = saxon.expr.instruct.GlobalParameterSet()
    params.put(saxon.om.StructuredQName('', '', 'marcxmluri'),
               xml_location)
    #eis = InputSource(File(xml_location).toURI().toString())
    #source_input = SAXSource(eis)
    #doc = config.buildDocument(source_input, None)
    static_env = config.newStaticQueryContext()
    #dynamic_env.setContextItem(doc)

    query_stream = FileInputStream(saxon_xqy)
    static_env.setBaseURI(File(saxon_xqy).toURI().toString())
    xquery_exp = static_env.compileQuery(query_stream, None)
    output_file = File(rdf_location)
    output_file.createNewFile()
    destination = FileOutputStream(output_file)
    dynamic_env = saxon.query.DynamicQueryContext(config)
    dynamic_env.setParameters(params)
    xquery_exp.run(dynamic_env,StreamResult(destination), None)

if __name__ == '__main__':

    # setup query processor
    PROCESSOR = XQueryProcessor(
        base_uri='file://C:/Users/jernelson/Development/marc2bibframe/xbin/saxon.xqy',
        saxon_xqy="C:\\Users\\jernelson\\Development\\marc2bibframe\\xbin\\saxon.xqy")

    # Run as a socket server
    HOST, PORT = "localhost", 8089
    server = SocketServer.TCPServer(
        (HOST, PORT),
        Marc2BibframeTCPHandler)
    print("Running xquery server at {} {}\nCtrl-C to stop".format(HOST, PORT))
    server.serve_forever()
