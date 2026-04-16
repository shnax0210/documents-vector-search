#!/usr/bin/env python3
import argparse
import os
import sys
from typing import List, Optional, Annotated
from pydantic import Field
from mcp.server.fastmcp import FastMCP

from main.factories.fetch_collection_factory import create_collection_fetcher
from main.factories.search_collection_factory import create_collection_searcher
from main.utils.formatting import format_object
import logging

from main.utils.logger import setup_root_logger

# Write to stderr for MCP, since in other case logs will be mixed with stdout and break communication between MCP and the tool adapter
setup_root_logger(use_stderr=True)

# Parse command line arguments
ap = argparse.ArgumentParser()
ap.add_argument("-host", "--host", required=False, default="127.0.0.1", help="Host to bind the HTTP server")
ap.add_argument("-port", "--port", required=False, type=int, default=8000, help="Port to bind the HTTP server")

args = vars(ap.parse_args())

# Constant for collections base path
COLLECTIONS_BASE_PATH = "./data/collections"

def get_available_collections() -> List[str]:
    """Get list of all available collections."""
    if not os.path.exists(COLLECTIONS_BASE_PATH):
        return []
    
    collections = []
    for item in os.listdir(COLLECTIONS_BASE_PATH):
        item_path = os.path.join(COLLECTIONS_BASE_PATH, item)
        if os.path.isdir(item_path):
            # Check if it has a manifest.json to confirm it's a valid collection
            manifest_path = os.path.join(item_path, "manifest.json")
            if os.path.exists(manifest_path):
                collections.append(item)
    
    return sorted(collections)

def create_collection_searcher_wrapper(collection_name: str, **kwargs):
    """Create a searcher for a specific collection with error handling."""
    try:
        return create_collection_searcher(
            collection_name=collection_name,
            index_names=kwargs.get('indexes'),
            filter=kwargs.get('filter'),
            rrf_k=kwargs.get('rrfK', 60)
        )
    except Exception as e:
        raise ValueError(f"Failed to create searcher for collection '{collection_name}': {str(e)}")

def create_collection_fetcher_wrapper(collection_name: str):
    """Create a fetcher for a specific collection with error handling."""
    try:
        return create_collection_fetcher(collection_name=collection_name)
    except Exception as e:
        raise ValueError(f"Failed to create fetcher for collection '{collection_name}': {str(e)}")

# Create MCP server with HTTP transport
mcp = FastMCP("documents-search-all-collections")

# Debug and configure settings for host validation
if hasattr(mcp, 'settings'):
    logging.debug("FastMCP settings attributes: %s", [attr for attr in dir(mcp.settings) if not attr.startswith('__')])
    # Try to configure allow_hosts to allow connections from any host
    for attr in dir(mcp.settings):
        if 'host' in attr.lower() or 'cors' in attr.lower() or 'origin' in attr.lower():
            logging.debug("Setting %s: %s", attr, getattr(mcp.settings, attr, 'N/A'))
            if attr == 'allow_hosts':
                mcp.settings.allow_hosts = ["*"]
            elif attr == 'allowed_hosts':
                mcp.settings.allowed_hosts = ["*"]
            elif attr == 'cors_origins':
                mcp.settings.cors_origins = ["*"]
            elif attr == 'cors':
                mcp.settings.cors = True

# Tool to list all available collections
@mcp.tool(name="list_available_collections", description="Lists all available document collections that can be searched. Typically Confluence, Jira, Documents and other documentation resources. Returns a JSON array of collection names.")
def list_collections() -> str:
    """List all available collections.

    Returns:
        str: JSON-formatted list of collection names. Each name corresponds to a directory under ./data/collections/ containing a manifest.json file.
    """
    collections = get_available_collections()
    return format_object(collections, 'json')

# Tool to search in a specific collection
tool_description = """The tool allows searching in a specific collection of documents by vector search.
Each result document contains a 'url' field. If a document is relevant, include its URL alongside the matching excerpt.

Supports hybrid search (vector + keyword), multi-index fusion (RRF), filtering (e.g., by space), and configurable output formatting.
"""



@mcp.tool(name="search_in_collection", description=tool_description)
def search_in_collection(
    collection_name: Annotated[str, Field(description="Name of the collection to search in (e.g., \"confluence\").")],
    query: Annotated[str, Field(description="The search query text (natural language or keywords).")],
    indexes: Annotated[Optional[str], Field(description="Comma-separated index names (e.g., \"default,metadata\") to search in. If omitted, all indexes are used.")] = None,
    filter: Annotated[Optional[str], Field(description="Optional filter expression (e.g., 'space = \"DEV\"') to narrow results.")] = None,
    rrfK: Annotated[int, Field(description="Reciprocal Rank Fusion constant for multi-index results fusion (default: 60).", ge=0)] = 60,
    maxNumberOfChunks: Annotated[int, Field(description="Maximum number of text chunks returned (default: 50).", ge=1)] = 50,
    maxNumberOfDocuments: Annotated[Optional[int], Field(description="Maximum number of *unique* documents returned (optional; overrides chunk-based limit).", ge=1)] = None,
    includeFullText: Annotated[bool, Field(description="If True, returns full content of matched documents (overrides chunk-level excerpts).")] = False,
    format: Annotated[str, Field(description="Output format — one of 'json', 'json_with_indent', or 'toon' (human-readable summary).", pattern="^(json|json_with_indent|toon)$")] = "toon"
) -> str:
    """Search in a specific collection by vector search.
    
    Args:
        collection_name: Name of the collection to search in (e.g., "confluence").
        query: The search query text (natural language or keywords).
        indexes: Comma-separated index names (e.g., "default,metadata") to search in. If omitted, all indexes are used.
        filter: Optional filter expression (e.g., 'space = "DEV"') to narrow results.
        rrfK: Reciprocal Rank Fusion constant for multi-index results fusion (default: 60).
        maxNumberOfChunks: Maximum number of text chunks returned (default: 50).
        maxNumberOfDocuments: Maximum number of *unique* documents returned (optional; overrides chunk-based limit).
        includeFullText: If True, returns full content of matched documents (overrides chunk-level excerpts).
        format: Output format — one of 'json', 'json_with_indent', or 'toon' (human-readable summary).

    Returns:
        str: Formatted search results with relevance-ranked chunks/documents and metadata (including URLs).
    """
    # Parse indexes from comma-separated string if provided
    index_list = None
    if indexes:
        index_list = [idx.strip() for idx in indexes.split(',')]
    
    searcher = create_collection_searcher_wrapper(
        collection_name=collection_name,
        indexes=index_list,
        filter=filter,
        rrfK=rrfK
    )
    
    # Debug: Log actual call being made
    logging.debug("DEBUG: Calling searcher.search(query=%r, max_number_of_chunks=%d, ...)", query, maxNumberOfChunks)

    # Try common alternative names if 'query' fails
    search_results = searcher.search(
        query,
        max_number_of_chunks=maxNumberOfChunks,
        max_number_of_documents=maxNumberOfDocuments,
        include_text_content=includeFullText,
        include_matched_chunks_content=not includeFullText
    )
    
    return format_object(search_results, format)

# Tool to fetch a document from a specific collection
fetch_tool_description = """The tool allows fetching a full document from a specific collection by its id.
Use startLine and endLine to read a specific portion of the document. If the document is too large, fetch it in parts.

`id` interpretation:
- For Confluence: page ID (numeric).
- For Jira: issue key (e.g., PROJ-123).
- For file-based collections: relative path (e.g., docs/guide.md).

You can extract the `id` from a URL — for example, from https://your-domain.atlassian.net/wiki/spaces/DEV/pages/123456, the page ID is 123456.
"""



@mcp.tool(name="fetch_from_collection", description=fetch_tool_description)
def fetch_from_collection(
    collection_name: Annotated[str, Field(description="Name of the collection to fetch from (e.g., \"confluence\").")],
    id: Annotated[str, Field(description="Document identifier as String type (e.g., Confluence page ID, Jira issue key, or file path).")],
    startLine: Annotated[int, Field(description="Starting line number to read (1-based, default: 1).", ge=1)] = 1,
    endLine: Annotated[int, Field(description="Ending line number to read (default: 250).", ge=1)] = 250,
    format: Annotated[str, Field(description="Output format — one of 'json', 'json_with_indent', or 'toon' (default).", pattern="^(json|json_with_indent|toon)$")] = "toon"
) -> str:
    """Fetch a document from a specific collection by its ID.
    
    Args:
        collection_name: Name of the collection to fetch from (e.g., "confluence").
        id: Document identifier (e.g., Confluence page ID, Jira issue key, or file path).
        startLine: Starting line number to read (1-based, default: 1).
        endLine: Ending line number to read (default: 250).
        format: Output format — one of 'json', 'json_with_indent', or 'toon' (default).

    Returns:
        str: Document content (or excerpt) in the requested format, with metadata (title, URL, etc.).
    """
    fetcher = create_collection_fetcher_wrapper(collection_name=collection_name)
    result = fetcher.fetch(id=id, start_line=startLine, end_line=endLine)
    return format_object(result, format)

if __name__ == "__main__":
    # Import uvicorn here to avoid errors if not needed
    import uvicorn
    
    # Run with HTTP transport using uvicorn directly with custom host/port
    logging.info(f"Starting MCP HTTP server on 0.0.0.0:{args['port']}")
    logging.info("MCP tools available:")
    logging.info("  - list_available_collections: Get list of all available collections")
    logging.info("  - search_in_collection: Search in a specific collection")
    logging.info("  - fetch_from_collection: Fetch a document from a specific collection")
    
    # Show available collections on startup
    collections = get_available_collections()
    logging.info(f"\nAvailable collections: {', '.join(collections) if collections else 'None found'}")
    
    # Get the ASGI app from FastMCP using the public streamable_http_app attribute
    app = mcp.streamable_http_app
    
    # Debug: Check if the app has middleware that validates Host headers
    if hasattr(app, 'middleware_stack'):
        logging.debug("\nApp middleware_stack type: %s", type(app.middleware_stack))
    if hasattr(app, 'routes'):
        logging.debug("App routes: %s", app.routes)
    
    # Try to configure the app to allow all hosts
    # This might require modifying the middleware or adding custom middleware
    logging.debug("\nConfiguring app to allow connections from any host...")
    
    # Run with uvicorn directly with the desired host and port
    logging.info(f"\nServer will be accessible at http://localhost:{args['port']}/mcp")
    logging.info(f"Server will be accessible at http://<your-ip>:{args['port']}/mcp")
    uvicorn.run(app, host=args['host'], port=args['port'], log_level="info", proxy_headers=True, forwarded_allow_ips="*")
