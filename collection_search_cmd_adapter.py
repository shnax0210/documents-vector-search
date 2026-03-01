import argparse
import logging

from main.utils.formatting import format_object
from main.utils.logger import setup_root_logger
from main.utils.performance import log_execution_duration
from main.factories.search_collection_factory import create_collection_searcher

setup_root_logger()

ap = argparse.ArgumentParser()
ap.add_argument("-collection", "--collection", required=True, help="Collection name (will be used as root folder name)")
ap.add_argument("-query", "--query", required=True, help="Text query for search")

ap.add_argument("-filter", "--filter", required=False, default=None, help="""Filter query for search. Uses common syntax: 'field operator "value"'. Multiple conditions can be combined with 'and'/'or'. Examples: --filter 'space = "SPACE_KEY"', --filter 'space = "SPACE_KEY" and lastModifiedAt > "2026-01-01"'""")

ap.add_argument("-indexes", "--indexes", nargs="+", required=False, default=None, help="Index(es) for search. Multiple can be specified (e.g. --indexes index1 index2). If not specified, all available indexes are used. Multiple indexes are combined using Reciprocal Rank Fusion.")
ap.add_argument("-rrfK", "--rrfK", required=False, type=int, default=60, help="RRF constant for multi-index search fusion. Higher values reduce rank impact.")

ap.add_argument("-maxNumberOfChunks", "--maxNumberOfChunks", required=False, type=int, default=None, help="Max number of text chunks in result")
ap.add_argument("-maxNumberOfDocuments", "--maxNumberOfDocuments", required=False, type=int, default=10, help="Max number of documents in result")

ap.add_argument("-includeFullText", "--includeFullText", action="store_true", required=False, default=False, help="If passed - full text content will be included in the search result.")
ap.add_argument("-includeAllChunksText", "--includeAllChunksText", action="store_true", required=False, default=False, help="If passed - all chunks text content will be included in the search result.")
ap.add_argument("-includeMatchedChunksText", "--includeMatchedChunksText", action="store_true", required=False, default=False, help="If passed - matched chunks text content will be included in the search result.")

ap.add_argument("-format", "--format", default="json_with_indent", required=False, choices=['json', 'json_with_indent', 'toon'], help="Output format for the search result (e.g., 'json', 'json_with_indent', 'toon')")
args = vars(ap.parse_args())

searcher = create_collection_searcher(collection_name=args['collection'], index_names=args['indexes'], filter=args['filter'], rrf_k=args['rrfK'])

max_number_of_chunks = args['maxNumberOfChunks'] if args['maxNumberOfChunks'] is not None else args['maxNumberOfDocuments'] * 3
search_result = log_execution_duration(lambda: searcher.search(args['query'],
                                                               max_number_of_chunks=max_number_of_chunks, 
                                                               max_number_of_documents=args['maxNumberOfDocuments'], 
                                                               include_text_content=args['includeFullText'], 
                                                               include_matched_chunks_content=args['includeMatchedChunksText'],
                                                               include_all_chunks_content=args['includeAllChunksText']),
                                       identifier=f"Searching collection: \"{args['collection']}\" by query: \"{args['query']}\"")

logging.info(f"Search results:\n{format_object(search_result, args['format'])}")