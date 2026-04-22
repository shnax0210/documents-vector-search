#!/usr/bin/env python3
import argparse
import json
import os
import sys
import threading
from typing import Annotated

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from main.factories.fetch_collection_factory import create_collection_fetcher
from main.factories.search_collection_factory import create_collection_searcher
from main.utils.formatting import format_object
from main.utils.logger import setup_root_logger

setup_root_logger(use_stderr=True)

ap = argparse.ArgumentParser()
ap.add_argument("-c", "--collections", nargs="*", default=None, help="Collections to search in. If not passed, all collections are available.")
ap.add_argument("-rrfK", "--rrfK", required=False, type=int, default=60, help="RRF constant for multi-index search fusion.")
ap.add_argument("-f", "--format", default="toon", required=False, choices=["json", "json_with_indent", "toon"], help="Output format.")
args = vars(ap.parse_args())

__COLLECTIONS_BASE_PATH = "./data/collections"

__COLLECTION_TYPE_MAP = {
    "confluence": "confluence",
    "confluenceCloud": "confluence",
    "jira": "jira",
    "jiraCloud": "jira",
    "localFiles": "files",
}

__FILTER_FIELDS_BY_TYPE = {
    "confluence": ["space", "createdAt", "createdBy", "lastModifiedAt"],
    "jira": ["project", "type", "status", "priority", "epic", "assignee", "createdAt", "createdBy", "lastModifiedAt"],
    "files": ["createdAt", "lastModifiedAt", "folder1", "folder2", "...", "folderN"],
}


def __discover_collections(allowed_names: list[str] | None) -> list[dict]:
    collections = []
    for name in sorted(os.listdir(__COLLECTIONS_BASE_PATH)):
        manifest_path = os.path.join(__COLLECTIONS_BASE_PATH, name, "manifest.json")
        if not os.path.isfile(manifest_path):
            continue
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        reader_type = manifest.get("reader", {}).get("type", "")
        collection_type = __COLLECTION_TYPE_MAP.get(reader_type)
        if not collection_type:
            continue
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
        print(f"Error: collections not found: {', '.join(sorted(missing))}", file=sys.stderr)
        sys.exit(1)


def __build_search_tool_description(collections: list[dict]) -> str:
    present_types = sorted({c["type"] for c in collections})

    filter_rows = "\n".join(
        f"| {t} | {', '.join(__FILTER_FIELDS_BY_TYPE[t])} |"
        for t in present_types
    )

    collection_rows = "\n".join(
        f"| {c['name']} | {c['type']} | {c['numberOfDocuments']} |"
        for c in collections
    )

    return f"""Search in a collection of documents by vector search.
Each document contains 'url' field, if you consider a document as relevant to the query, always include the 'url' field in the response, put it close to the information used from the document.

Available collections:

| Collection name | Collection type | Number of documents |
|---|---|---|
{collection_rows}

Filter fields by collection type:

| Collection type | Filter fields |
|---|---|
{filter_rows}

Filter syntax: `field operator "value"`. Operators: =, !=, >, >=, <, <=. Combine conditions with `and` / `or`. Use parentheses for grouping.
Examples:
- space = "SPACE_KEY"
- space = "SPACE_KEY" and lastModifiedAt > "2026-01-01"
- (space = "X" or space = "Y") and createdBy = "user"
"""


__FETCH_TOOL_DESCRIPTION = """Fetch a full document from a collection by its id.
Use startLine and endLine to read a specific portion of the document. If document is too large, fetch it in parts.

`id` means:
- For Confluence: page id.
- For Jira: issue key (e.g. PROJ-123).
- For files collection: relative path.

Users often provides url to the document, extract id from the url and use it to fetch the document in the case.

Available collections are listed in the description of search tool.
"""

discovered = __discover_collections(args["collections"])

if args["collections"] is not None:
    __validate_collections(args["collections"], discovered)

if not discovered:
    print("Error: no collections found.", file=sys.stderr)
    sys.exit(1)

mcp = FastMCP("documents-search-unified")

search_description = __build_search_tool_description(discovered)
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


@mcp.tool(name="search_in_collection", description=search_description)
def search_in_collection(
    collectionName: Annotated[str, Field(description="Collection name must be one of the available collections listed above.")],
    query: Annotated[str, Field(description="Search query text for vector similarity and keyword search.", default="")],
    filter: Annotated[str | None, Field(description="Filter expression to narrow results. See filter syntax above.", default=None)],
    maxNumberOfChunks: Annotated[int, Field(description="Maximum number of document chunks to return. Prefer to use default value unless there is strong reason to change", default=50)],
) -> str:
    if collectionName not in available_names:
        return f"Error: collection '{collectionName}' is not available. Available: {', '.join(sorted(available_names))}"
    if not query and not filter:
        return "Error: at least one of 'query' or 'filter' must be provided."

    search_results = __get_or_create_searcher(collectionName).search(
        query or "",
        max_number_of_chunks=maxNumberOfChunks,
        include_matched_chunks_content=True,
        filter=filter or None,
    )
    return format_object(search_results, args["format"])


@mcp.tool(name="fetch_from_collection", description=__FETCH_TOOL_DESCRIPTION)
def fetch_from_collection(
    collectionName: Annotated[str, Field(description="Name of the collection to fetch from. Must be one of the available collections.")],
    id: Annotated[str, Field(description="Document identifier. Confluence: page id. Jira: issue key (e.g. PROJ-123). Files: relative path.")],
    startLine: Annotated[int, Field(description="First line number to return (1-based, inclusive). Default: 1.", default=1)],
    endLine: Annotated[int, Field(description="Last line number to return (1-based, inclusive). Default: 250.", default=250)],
) -> str:
    if collectionName not in available_names:
        return f"Error: collection '{collectionName}' is not available. Available: {', '.join(sorted(available_names))}"

    fetcher = create_collection_fetcher(collection_name=collectionName)
    result = fetcher.fetch(id=id, start_line=startLine, end_line=endLine)
    return format_object(result, args["format"])


mcp.run(transport="stdio")
