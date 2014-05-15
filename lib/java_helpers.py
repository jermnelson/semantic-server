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
import argparse
import datetime
import json
import os
import SocketServer
import sys
import tempfile
import urllib

for filename in os.listdir("."):
    if os.path.splitext(filename)[-1].endswith(".jar"):
        sys.path.append(filename)

from java.io import File, ByteArrayInputStream, ByteArrayOutputStream
from java.io import FileInputStream, FileOutputStream, PrintStream, StringReader
from java.lang import System
from java.net import URI
from java.util import Properties
from javax.xml.parsers import DocumentBuilderFactory
from javax.xml.transform.stream import StreamResult, StreamSource
from javax.xml.transform.sax import SAXSource
from net.sf import saxon
from org.xml.sax import InputSource
from StringIO import StringIO
from tempfile import NamedTemporaryFile

PROCESSOR = None

def test_ingester(marc_xmlfile):
    base_uri = 'http://catalog/'
    args = ["C:\\Users\\jernelson\\Development\\marc2bibframe\\xbin\\saxon.xqy",
            'marcxmluri={}'.format(
                os.path.normpath(marc_xmlfile).replace("\\", "/")),
            'baseuri={}'.format(base_uri),
            'serialization=rdfxml']
    query = saxon.Query()
    sys.stdout = StringIO()
    System.setOut(sys.stdout)
    query.main(args)
    result = sys.stdout.getvalue()
    return result

class XQueryProcessor(object):

    def __init__(self, **kwargs):
        self.query = saxon.Query()
        self.base_uri = kwargs.get('base_uri', 'http://catalog/')
        self.config = saxon.Configuration.newConfiguration()
        self.static_env = self.config.newStaticQueryContext()
        query_stream = FileInputStream(kwargs.get('saxon_xqy'))
        self.static_env.setBaseURI(File(kwargs.get('saxon_xqy')).toURI().toString())
        self.expression = self.static_env.compileQuery(query_stream, None)


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
        source_input = StreamSource(StringReader(marc_xml))
##        factory = DocumentBuilderFactory.newInstance()
##        builder = factory.newDocumentBuilder()
##        doc = builder.parse(source_input)
            #File(marc_xml).toURI().toString())
        dynamic_env = saxon.query.DynamicQueryContext(self.config)
        options = saxon.lib.ParseOptions(self.config.getParseOptions())
        doc = self.config.buildDocument(source_input)
        dynamic_env.setContextItem(doc)
        dynamic_env.setParameter('baseuri', 'self.base_uri')
        dynamic_env.setParameter('raw_marcxml', marc_xml)
        dynamic_env.setParameter('serialization', 'rdfxml')
        output_stream = StreamResult(ByteArrayOutputStream())
        self.expression.run(dynamic_env,
                            output_stream,
                            None)
##        output_file = NamedTemporaryFile(delete=False)
##        args = [self.saxon_xqy,
##                'marcxmluri={}'.format(marc_xml),
##                'baseuri={}'.format(self.base_uri),
##                'serialization=rdfxml']
##        result = self.query.main(args)
##        self.xquery_evaluator.setExternalVariable(
##            saxon.s9api.QName('marcxmluri'),
##            saxon.s9api.XdmAtomicValue(marc_xml))
        #source = SAXSource(InputSource(marc_xml))
        #source.setSystemId('http://catalog/')
##        self.xquery_evaluator.setExternalVariable(
##            saxon.s9api.QName("marcxmluri"),
##            saxon.s9api.XdmAtomicValue(marc_xml))
##        out = self.processor.newSerializer(System.out)
##        self.xquery_evaluator.run(out)
##        print("After result={}".format(result))

##        dynamic_env = saxon.query.DynamicQueryContext(self.config)
##        params = saxon.expr.instruct.GlobalParameterSet()
##        params.put(
##            saxon.om.StructuredQName('', '', 'marcxmluri'),
##            marc_xml)
##        dynamic_env.setParameters(params)
##        result = self.xquery_exp.run(dynamic_env)
        return output_stream.getOutputStream().toString()

class Marc2BibframeTCPHandler(SocketServer.StreamRequestHandler):

    def handle(self):
        try:
            marc_xmlfile = self.request.recv(1014).strip()

            base_uri = 'http://catalog/'
    #        output_file = NamedTemporaryFile(delete=False)
            args = ["C:\\Users\\jernelson\\Development\\marc2bibframe\\xbin\\saxon.xqy",
    #                "-o:{}".format(output_file.name),
                    'marcxmluri={}'.format(
                    os.path.normpath(marc_xmlfile).replace("\\", "/")),
                    'baseuri={}'.format(base_uri),
                    'serialization=rdfxml']
            query = saxon.Query()
            output_stream = ByteArrayOutputStream()
            System.setOut(PrintStream(output_stream))
            query.main(args)
            self.wfile.write(output_stream.toString().encode('ascii',
                                                         errors='ignore'))
        except:
            self.wfile.write("Error processing MARC XML:\n\t{}".format(sys.exc_info()[0]))
##        output_file.close()
##        self.request.sendall(output_file.name)



    def stream_handle(self):
##        config = saxon.Configuration.newConfiguration()
##        dynamic_env = saxon.query.DynamicQueryContext(config)
##        params = saxon.expr.instruct.GlobalParameterSet()
##        params.put(saxon.om.StructuredQName('', '', 'marcxmluri'),
##                   xml_location)
##        dynamic_env.setParameters(params)
        self.data = self.rfile.readline().strip()
        self.data = urllib.unquote(self.data)

        rdf_output = PROCESSOR.run(self.data)
        print("{} wrote XML {}".format(self.client_address[0],
            rdf_output))
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
    parser = argparse.ArgumentParser()
    parser.add_argument('host',
                         help='Host for xquery server',
                         default='localhost')
    parser.add_argument('port',
                        type=int,
                        help='Port for xquery server',
                        default=8089)
    args = parser.parse_args()
    # setup query processor
    PROCESSOR = XQueryProcessor(
        base_uri='file://C:/Users/jernelson/Development/marc2bibframe/xbin/saxon-stream.xqy',
        saxon_xqy="C:\\Users\\jernelson\\Development\\marc2bibframe\\xbin\\saxon-stream.xqy")

    # Run as a socket server
    server = SocketServer.TCPServer(
        (args.host, args.port),
        Marc2BibframeTCPHandler)
    print("Running xquery server at {} {}\nCtrl-C to stop".format(args.host,
                                                                  args.port))
    server.serve_forever()

