#!/usr/bin/env python3
import json
import argparse

from mcp.server.fastmcp import FastMCP

from main.factories.search_collection_factory import create_collection_searcher
from main.utils.logger import setup_root_logger

# Write to stderr for MCP, since in other case logs will be mixed with stdout and break communication between MCP and the tool adapter
setup_root_logger(use_stderr=True)

mcp = FastMCP("documents-search")

ap = argparse.ArgumentParser()
ap.add_argument("-collection", "--collection", required=True, help="Collection name (will be used as root folder name)")

ap.add_argument("-indexes", "--indexes", nargs="+", required=False, default=None, help="Index(es) for search. Multiple can be specified (e.g. --indexes index1 index2). If not specified, all available indexes are used. Multiple indexes are combined using Reciprocal Rank Fusion.")
ap.add_argument("-rrfK", "--rrfK", required=False, type=int, default=60, help="RRF constant for multi-index search fusion. Higher values reduce rank impact.")

ap.add_argument("-filter", "--filter", required=False, default=None, help="""Filter query for search. Uses common syntax: 'field operator "value"'. Multiple conditions can be combined with 'and'/'or'. Examples: --filter 'space = "SPACE_KEY"', --filter 'space = "SPACE_KEY" and lastModifiedAt > "2026-01-01"'""")

ap.add_argument("-maxNumberOfChunks", "--maxNumberOfChunks", required=False, type=int, default=100, help="Max number of text chunks in result")
ap.add_argument("-maxNumberOfDocuments", "--maxNumberOfDocuments", required=False, type=int, default=None, help="Max number of documents in result")

ap.add_argument("-includeFullText", "--includeFullText", action="store_true", required=False, default=False, help="If passed - full text content will be included in the search result. By default only matched chunks content is included. If passed, it's better to reduce --maxNumberOfChunks or set small --maxNumberOfDocuments like 10-30 to avoid too big response and breaking AI agent.")
args = vars(ap.parse_args())

searcher = create_collection_searcher(collection_name=args['collection'], index_names=args['indexes'], filter=args['filter'], rrf_k=args['rrfK'])

tool_description = """The tool allows searching in collection of documents by vector search. 
Each document contains 'url' field, if you consider a document as relevant to the query, always include the 'url' field in the response, put it close to the information used from the document"""

@mcp.tool(name=f"search_{args['collection']}", description=tool_description)
def search_documents(query: str) -> str:
    search_results = searcher.search(query, 
                                     max_number_of_chunks=args['maxNumberOfChunks'], 
                                     max_number_of_documents=args['maxNumberOfDocuments'],
                                     include_text_content=args['includeFullText'],
                                     include_matched_chunks_content=not args['includeFullText'])

    return json.dumps(search_results, indent=2, ensure_ascii=False)
    

if __name__ == "__main__":
    mcp.run(transport='stdio')