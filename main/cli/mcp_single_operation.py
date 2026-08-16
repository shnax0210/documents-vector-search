import argparse

from mcp.server.fastmcp import FastMCP

from main.cli.base_cli_operation import BaseCliOperation
from main.cli.cli_arguments import add_collection_argument, add_filter_argument, add_format_argument, add_indexes_argument, add_rrf_k_argument
from main.factories.fetch_collection_factory import create_collection_fetcher
from main.factories.search_collection_factory import create_collection_searcher
from main.utils.formatting import format_object

SEARCH_TOOL_DESCRIPTION = """The tool allows searching in collection of documents by vector search. 
Each document contains 'url' field, if you consider a document as relevant to the query, always include the 'url' field in the response, put it close to the information used from the document"""

FETCH_TOOL_DESCRIPTION = """The tool allows fetching a full document from the collection by its id.
Use startLine and endLine to read a specific portion of the document. If document is too large, fetch it in parts.

`id` means:
- For Confluence: page id.
- For Jira: issue key (e.g. PROJ-123).
- For files collection: relative path.

Users often provides url to the document, extract id from the url and use it to fetch the document in the case.
"""


class McpSingleOperation(BaseCliOperation):
    def name(self) -> str:
        return "mcp-single"

    def help(self) -> str:
        return "Run MCP stdio server for a single collection (search and fetch settings are fixed via arguments)"

    def configure(self, parser: argparse.ArgumentParser) -> None:
        add_collection_argument(parser)

        add_indexes_argument(parser)
        add_rrf_k_argument(parser)
        add_filter_argument(parser)

        parser.add_argument("-n", "--maxNumberOfChunks", required=False, type=int, default=50, help="Max number of text chunks in result")
        parser.add_argument("-d", "--maxNumberOfDocuments", required=False, type=int, default=None, help="Max number of documents in result")

        parser.add_argument("-t", "--includeFullText", action="store_true", required=False, default=False,
                            help="If passed - full text content will be included in the search result. By default only matched chunks content is included. If passed, it's better to reduce --maxNumberOfChunks or set small --maxNumberOfDocuments like 10-30 to avoid too big response and breaking AI agent.")

        add_format_argument(parser, default="toon")

    def log_to_stderr(self, args: dict) -> bool:
        return True

    def run(self, args: dict) -> None:
        searcher = create_collection_searcher(collection_name=args["collection"], index_names=args["indexes"], rrf_k=args["rrfK"])
        fetcher = create_collection_fetcher(collection_name=args["collection"])

        mcp = FastMCP("documents-search")

        @mcp.tool(name=f"search_in_{args['collection']}", description=SEARCH_TOOL_DESCRIPTION)
        def search_documents(query: str) -> str:
            search_results = searcher.search(query,
                                             max_number_of_chunks=args["maxNumberOfChunks"],
                                             max_number_of_documents=args["maxNumberOfDocuments"],
                                             include_text_content=args["includeFullText"],
                                             include_matched_chunks_content=not args["includeFullText"],
                                             filter=args["filter"])

            return format_object(search_results, args["format"])

        @mcp.tool(name=f"fetch_from_{args['collection']}", description=FETCH_TOOL_DESCRIPTION)
        def fetch_document(id: str, startLine: int = 1, endLine: int = 250) -> str:
            result = fetcher.fetch(id=id, start_line=startLine, end_line=endLine)
            return format_object(result, args["format"])

        mcp.run(transport="stdio")
