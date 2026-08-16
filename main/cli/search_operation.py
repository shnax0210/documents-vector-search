import argparse
import logging

from main.cli.base_cli_operation import BaseCliOperation
from main.cli.cli_arguments import add_collection_argument, add_filter_argument, add_format_argument, add_indexes_argument, add_rrf_k_argument
from main.factories.search_collection_factory import create_collection_searcher
from main.utils.formatting import format_object
from main.utils.performance import log_execution_duration


class SearchOperation(BaseCliOperation):
    def name(self) -> str:
        return "search"

    def help(self) -> str:
        return "Search in a collection by text query and/or metafields filter"

    def configure(self, parser: argparse.ArgumentParser) -> None:
        add_collection_argument(parser)

        parser.add_argument("-q", "--query", required=True, help="Text query for search")

        add_filter_argument(parser)
        add_indexes_argument(parser)
        add_rrf_k_argument(parser)

        parser.add_argument("-n", "--maxNumberOfChunks", required=False, type=int, default=None, help="Max number of text chunks in result")
        parser.add_argument("-d", "--maxNumberOfDocuments", required=False, type=int, default=10, help="Max number of documents in result")

        parser.add_argument("-t", "--includeFullText", action="store_true", required=False, default=False,
                            help="If passed - full text content will be included in the search result.")
        parser.add_argument("-a", "--includeAllChunksText", action="store_true", required=False, default=False,
                            help="If passed - all chunks text content will be included in the search result.")
        parser.add_argument("-m", "--includeMatchedChunksText", action="store_true", required=False, default=False,
                            help="If passed - matched chunks text content will be included in the search result.")

        add_format_argument(parser)

    def run(self, args: dict) -> None:
        searcher = create_collection_searcher(collection_name=args["collection"], index_names=args["indexes"], rrf_k=args["rrfK"])

        max_number_of_chunks = args["maxNumberOfChunks"] if args["maxNumberOfChunks"] is not None else args["maxNumberOfDocuments"] * 3

        search_result = log_execution_duration(lambda: searcher.search(args["query"],
                                                                      max_number_of_chunks=max_number_of_chunks,
                                                                      max_number_of_documents=args["maxNumberOfDocuments"],
                                                                      include_text_content=args["includeFullText"],
                                                                      include_matched_chunks_content=args["includeMatchedChunksText"],
                                                                      include_all_chunks_content=args["includeAllChunksText"],
                                                                      filter=args["filter"]),
                                              identifier=f"Searching collection: \"{args['collection']}\" by query: \"{args['query']}\"")

        logging.info(f"Search results:\n{format_object(search_result, args['format'])}")
