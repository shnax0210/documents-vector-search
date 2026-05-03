#!/usr/bin/env python3
import argparse
import json
import os
import threading
from typing import Annotated

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from main.factories.fetch_collection_factory import create_collection_fetcher
from main.factories.search_collection_factory import create_collection_searcher
from main.utils.formatting import format_object
from main.utils.logger import setup_root_logger

ap = argparse.ArgumentParser()
ap.add_argument("-c", "--collections", nargs="*", default=None, help="Collections to search in. If not passed, all collections are available.")
ap.add_argument("-rrfK", "--rrfK", required=False, type=int, default=60, help="RRF constant for multi-index search fusion.")
ap.add_argument("-f", "--format", default="toon", required=False, choices=["json", "json_with_indent", "toon"], help="Output format.")

ap.add_argument("--defaultNumberOfChunks", type=int, default=50, help="Default number of chunks returned by search (default: 50).")
ap.add_argument("--maxNumberOfChunks", type=int, default=100, help="Maximum allowed number of chunks for search (default: 100).")

ap.add_argument("--http", action="store_true", default=False, help="Run MCP server over HTTP (streamable-http) instead of stdio.")
ap.add_argument("--http-port", type=int, default=8000, help="Port for HTTP transport (default: 8000).")
args = vars(ap.parse_args())

transport = "streamable-http" if args["http"] else "stdio"

setup_root_logger(use_stderr=(transport == "stdio"))

COLLECTIONS_BASE_PATH = "./data/collections"

COLLECTION_TYPE_MAP = {
    "confluence": "confluence",
    "confluenceCloud": "confluence",
    "jira": "jira",
    "jiraCloud": "jira",
    "localFiles": "files",
}

FILTER_FIELDS_BY_TYPE = {
    "confluence": ["space", "createdAt", "createdBy", "lastModifiedAt"],
    "jira": ["project", "type", "status", "priority", "epic", "assignee", "createdAt", "createdBy", "lastModifiedAt"],
    "files": ["createdAt", "lastModifiedAt", "folder1", "folder2", "...", "folderN"],
}

def __read_json_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def __discover_collections(allowed_names: list[str] | None) -> list[dict]:
    collections = []
    for name in sorted(os.listdir(COLLECTIONS_BASE_PATH)):
        collection_path = os.path.join(COLLECTIONS_BASE_PATH, name)
        manifest_path = os.path.join(collection_path, "manifest.json")

        if not os.path.isfile(manifest_path):
            continue

        manifest = __read_json_file(manifest_path)

        reader_type = manifest.get("reader", {}).get("type", "")
        collection_type = COLLECTION_TYPE_MAP.get(reader_type)

        if not collection_type:
            raise ValueError(f"Unknown reader type '{reader_type}' for collection '{name}' in manifest.json. Supported types: {COLLECTION_TYPE_MAP}")

        if allowed_names is not None and name not in allowed_names:
            continue
        collections.append({
            "name": name,
            "type": collection_type,
            "numberOfDocuments": manifest.get("numberOfDocuments"),
        })
    return collections

def __validate_collections(allowed_names: list[str], discovered: list[dict]) -> None:
    discovered_names = {c["name"] for c in discovered}
    missing = set(allowed_names) - discovered_names
    if missing:
        raise ValueError(f"Error: collections not found: {', '.join(sorted(missing))}, available: {', '.join(sorted(discovered_names))}")

def __build_search_tool_description(collections: list[dict]) -> str:
    return """Search in a collection of documents.

Typical use cases:
- User asks to search in a specific collection;
- User asks to search in a system for which there is a dedicated collection (e.g. "search in Confluence/Jira/Files"). In this case, you must to select most relevant collection for the system if there are several.

Each document contains 'url' field, if you consider a document as relevant to the query, always include the 'url' field in the response, put it close to the information used from the document.
"""

def __build_collection_field_description(collections: list[dict]) -> str:
    collection_rows = "\n".join(
        f"| {c['name']} | {c['type']} |"
        for c in collections
    )

    return f"""Collection name must be one of below:

| Collection name | Collection type |
|---|---|
{collection_rows}
"""

def __build_filter_field_description(collections: list[dict]) -> str:
    present_types = sorted({c["type"] for c in collections})

    filter_rows = "\n".join(
        f"| {t} | {', '.join(FILTER_FIELDS_BY_TYPE[t])} |"
        for t in present_types
    )

    return f"""Filter expression to narrow results.

Each collection type supports different fields:

| Collection type | Filter fields |
|---|---|
{filter_rows}

Filter syntax: `field operator "value"`. Operators: =, !=, >, >=, <, <=. Combine conditions with `and` / `or`. Use parentheses for grouping.
Examples:
- space = "SPACE_KEY"
- space = "SPACE_KEY" and lastModifiedAt > "2026-01-01"
- (space = "X" or space = "Y") and createdBy = "user"
"""

FETCH_TOOL_DESCRIPTION = """Fetch a document content from a collection by its id.

Typical use cases:
- User asks something and provides an id or url (fetch id from the url in the case) of a document (it can be Confluence page, jira ticket or file path) - fetch the document by the tool and use as a context.
- After using search_in_collection tool, you need more context from a found document.
"""

discovered = __discover_collections(args["collections"])

if args["collections"] is not None:
    __validate_collections(args["collections"], discovered)

if not discovered:
    raise ValueError("Error: no collections found.")

search_description = __build_search_tool_description(discovered)
collection_field_description = __build_collection_field_description(discovered)
filter_field_description = __build_filter_field_description(discovered)
available_names = {c["name"] for c in discovered}

searcher_cache = {}
searcher_cache_lock = threading.Lock()

def __get_or_create_searcher(collectionName: str):
    if collectionName in searcher_cache:
        return searcher_cache[collectionName]

    with searcher_cache_lock:
        if collectionName not in searcher_cache:
            searcher_cache[collectionName] = create_collection_searcher(
                collection_name=collectionName,
                rrf_k=args["rrfK"],
            )

    return searcher_cache[collectionName]

mcp = FastMCP("documents-search-unified", port=args["http_port"])

@mcp.tool(name="search_in_collection", description=search_description)
def search_in_collection(
    collection: Annotated[str, Field(description=collection_field_description)],
    query: Annotated[str, Field(description="Search query text for vector similarity and keyword search.", default="")],
    filter: Annotated[str | None, Field(description=filter_field_description, default=None)],
    numberOfChunks: Annotated[int, Field(description=f"Number of best matched document chunks to return. Prefer to use default value unless there is strong reason to change. Max allowed: {args['maxNumberOfChunks']}.", default=args["defaultNumberOfChunks"])],
) -> str:
    if collection not in available_names:
        return f"Error: collection '{collection}' is not available. Available: {', '.join(sorted(available_names))}"
    if not query and not filter:
        return "Error: at least one of 'query' or 'filter' must be provided."
    if numberOfChunks > args["maxNumberOfChunks"]:
        return f"Error: numberOfChunks ({numberOfChunks}) exceeds maximum allowed ({args['maxNumberOfChunks']})."

    search_results = __get_or_create_searcher(collection).search(
        query or "",
        max_number_of_chunks=numberOfChunks,
        include_matched_chunks_content=True,
        filter=filter or None,
    )
    return format_object(search_results, args["format"])

@mcp.tool(name="fetch_from_collection", description=FETCH_TOOL_DESCRIPTION)
def fetch_from_collection(
    collection: Annotated[str, Field(description=collection_field_description)],
    id: Annotated[str, Field(description="Document identifier. Confluence: page id. Jira: issue key (e.g. PROJ-123). Files: relative path.")],
    startLine: Annotated[int, Field(description="First line number to return (1-based, inclusive)", default=1)],
    endLine: Annotated[int, Field(description="Last line number to return (1-based, inclusive)", default=250)],
) -> str:
    if collection not in available_names:
        return f"Error: collection '{collection}' is not available. Available: {', '.join(sorted(available_names))}"

    fetcher = create_collection_fetcher(collection_name=collection)
    result = fetcher.fetch(id=id, start_line=startLine, end_line=endLine)
    return format_object(result, args["format"])

mcp.run(transport=transport)
