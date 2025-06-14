import json
import argparse
import logging

from main.utils.logger import setup_root_logger
from main.utils.performance import log_execution_duration
from main.factories.search_collection_factory import create_collection_searcher

setup_root_logger()

ap = argparse.ArgumentParser()
ap.add_argument("-collection", "--collection", required=True, help="Collection name (will be used as root folder name)")
ap.add_argument("-query", "--query", required=True, help="Text query for search")

ap.add_argument("-index", "--index", required=False, default="indexer_FAISS_IndexFlatL2__embeddings_all-MiniLM-L6-v2", help="Index that will be used for search")

ap.add_argument("-maxNumberOfChunks", "--maxNumberOfChunks", required=False, type=int, default=None, help="Max number of text chunks in result")
ap.add_argument("-maxNumberOfDocuments", "--maxNumberOfDocuments", required=False, type=int, default=10, help="Max number of documents in result")

ap.add_argument("-includeFullText", "--includeFullText", action="store_true", required=False, default=False, help="If passed - full text content will be included in the search result.")
ap.add_argument("-includeAllChunksText", "--includeAllChunksText", action="store_true", required=False, default=False, help="If passed - all chunks text content will be included in the search result.")
ap.add_argument("-includeMatchedChunksText", "--includeMatchedChunksText", action="store_true", required=False, default=False, help="If passed - matched chunks text content will be included in the search result.")
args = vars(ap.parse_args())

searcher = create_collection_searcher(collection_name=args['collection'], index_name=args['index'])

max_number_of_chunks = args['maxNumberOfChunks'] if args['maxNumberOfChunks'] is not None else args['maxNumberOfDocuments'] * 3
search_result = log_execution_duration(lambda: searcher.search(args['query'],
                                                               max_number_of_chunks=max_number_of_chunks, 
                                                               max_number_of_documents=args['maxNumberOfDocuments'], 
                                                               include_text_content=args['includeFullText'], 
                                                               include_matched_chunks_content=args['includeMatchedChunksText'],
                                                               include_all_chunks_content=args['includeAllChunksText']),
                                       identifier=f"Searching collection: \"{args['collection']}\" by query: \"{args['query']}\"")

logging.info(f"Search results:\n{json.dumps(search_result, indent=2, ensure_ascii=False)}")