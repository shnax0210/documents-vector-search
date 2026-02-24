import argparse
import os

from main.utils.logger import setup_root_logger
from main.sources.files.files_document_reader import FilesDocumentReader
from main.sources.files.files_document_converter import FilesDocumentConverter
from main.factories.create_collection_factory import create_collection_creator
from main.splitter.text_splitter import TextSplitter

setup_root_logger()

ap = argparse.ArgumentParser()
ap.add_argument("-collection", "--collection", required=False, help="Collection name (will be used as root folder name). If not provided, it will be derived from the basePath folder name.")

ap.add_argument("-basePath", "--basePath", required=True, help="Path to the root folder from which files will be read.")
ap.add_argument("-includePatterns", "--includePatterns", required=False, default=[".*"], help="List of file patterns to include into collection", nargs='+')
ap.add_argument("-excludePatterns", "--excludePatterns", required=False, default=[], help="List of file patterns to NOT include into collection", nargs='+')

ap.add_argument("-indexers", "--indexers", required=False, default=["indexer_ChromaDb__embeddings_all-MiniLM-L6-v2", "indexer_SqlLiteBM25"], help="List on indexer names", nargs='+')

ap.add_argument("-failFast", "--failFast", action="store_true", required=False, default=False, help="If passed - the process will stop on the first error. Otherwise, it will try to process all files and log errors for those that failed.")
ap.add_argument("-chunkSize", "--chunkSize", required=False, default=1000, type=int, help="Chunk size for text splitting (default: 1000)")
ap.add_argument("-chunkOverlap", "--chunkOverlap", required=False, default=100, type=int, help="Chunk overlap for text splitting (default: 100)")
args = vars(ap.parse_args())

text_splitter = TextSplitter(chunk_size=args['chunkSize'], chunk_overlap=args['chunkOverlap'])

files_document_reader = FilesDocumentReader(base_path=args['basePath'], 
                                            include_patterns=args['includePatterns'], 
                                            exclude_patterns=args['excludePatterns'],
                                            fail_fast=args['failFast'])
files_document_converter = FilesDocumentConverter(text_splitter)

collection_name = args['collection'] if args['collection'] else os.path.basename(args['basePath'])
files_collection_creator = create_collection_creator(collection_name=collection_name,
                                                     indexers=args['indexers'],
                                                     document_reader=files_document_reader,
                                                     document_converter=files_document_converter,
                                                     use_cache=False)

files_collection_creator.run()


