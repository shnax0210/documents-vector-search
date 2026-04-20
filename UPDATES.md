# Updates

## 2026/04/20
- Fixed https://github.com/shnax0210/documents-vector-search/issues/15
- Improved Confluence update query to ave only 5 mins of overlap between two updates;

## 2026/04/07
- Improved Jira incremental indexing:
	- Collection updates now use precise Jira watermark query (`updated >= "YYYY/MM/DD HH:mm"`) with a short overlap window by default;

## 2026/04/02
- Removed ability to fetch document by url, since it causes issues when user provides an url that a bit different from the one in a collection;
- Extracted updates to seprate file;
- Reduced number of returned chunks to 50 from 100 in MCP, since it looks like 50 is enough for most of searches.

## 2026/03/26 — ChromaDB collection folder storage improvment
- Updated ChromaDB indexer to avoid temp folder usage in runtime and store data in native ChromaDB format directly in collection folder without any transformation. It also improves performace.
- Existing collections will be automatically migrated to the new format durign first usage.

## 2026/02/24 - More embedding models, TOON format for MCP, fetch document script and tool, subfolders metadata
- Ability to use any embedding model form next [list](https://huggingface.co/models?pipeline_tag=sentence-similarity&library=sentence-transformers&sort=trending). Check [How it works](#how-it-works) section for details;
- MCP now supports [TOON](https://github.com/toon-format/toon) format for MCP;
- `collection_fetch_cmd_adapter.py` and MCP tool to fetch document from collection by id or url. For example, the tool can be useful when you need to find similar Jira ticket or Confluence to some existing one;
- Subfolders metadata for files.

## 2026/02/24 — Faster Chroma deserialization (interface preserved)
- New Chroma index payload format stores/restores the underlying Chroma storage directly, avoiding full Python-level embeddings replay during load and end up with significant performance gain (x2-x20 depends from a case);
- Existing collections remain supported (backward-compatible load path for previous payload format).

## 2026/02/22 — SQLite BM25, Reciprocal Rank Fusion, common filter syntax
- BM25 keyword search via SQLite
- Multi-index search with Reciprocal Rank Fusion
- Common filter syntax for ChromaDB and SQLite: `--filter 'space = "SPACE_KEY" and lastModifiedAt > "2026-01-01"'`

## 2026/01/25 — ChromaDB and metafields filtering

ChromaDB added as default vector database (replaces FAISS) with metafield filtering support. To use FAISS instead, pass `--indexes "indexer_FAISS_IndexFlatL2__embeddings_all-MiniLM-L6-v2"` during collection creation.
