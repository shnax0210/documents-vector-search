import os
import argparse

from main.sources.confluence.confluence_document_reader import ConfluenceDocumentReader
from main.sources.confluence.confluence_document_converter import ConfluenceDocumentConverter
from main.factories.create_collection_factory import create_collection_creator

ap = argparse.ArgumentParser()
ap.add_argument("-url", "--url", required=True, help="confluence base url")
ap.add_argument("-cql", "--cql", required=True, help="confluence query to get tickets for label adding")
ap.add_argument("-collection", "--collection", required=True, help="collaction name (will be used as root folder name)")
ap.add_argument("-indexers", "--indexers", required=False, default=["indexer_FAISS_IndexFlatL2__embeddings_all-MiniLM-L6-v2"], help="list on indexer names", nargs='+')
ap.add_argument("-readComments", "--readComments", required=False, type=bool, default=True, help="should page comments be loaded or not (it provides more info, but degradete performace of index creation/update much)")
args = vars(ap.parse_args())

token = os.environ.get('CONF_TOKEN')
login=os.environ.get('CONF_LOGIN')
password=os.environ.get('CONF_PASSWORD')

if not token and (not login or not password):
    raise ValueError("Either 'token' ('CONF_TOKEN' env variable) or both 'login' ('CONF_LOGIN' env variable) and 'password' ('CONF_PASSWORD' env variable) must be provided.")

confluence_document_reader = ConfluenceDocumentReader(base_url=args['url'],
                                                      query=args['cql'],
                                                      token=token,
                                                      login=login, 
                                                      password=password,
                                                      read_comments=args['readComments'],)
confluence_document_converter = ConfluenceDocumentConverter()

confluence_collection_creator = create_collection_creator(collection_name=args['collection'],
                                                          indexers=args['indexers'],
                                                          document_reader=confluence_document_reader,
                                                          document_converter=confluence_document_converter)

confluence_collection_creator.update_collection()