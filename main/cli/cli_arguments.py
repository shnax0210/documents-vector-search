import argparse

DEFAULT_INDEXERS = ["indexer_ChromaDb__embeddings_sentence-transformers_slash_all-MiniLM-L6-v2", "indexer_SqlLiteBM25"]

FORMAT_CHOICES = ["json", "json_with_indent", "toon"]


def add_collection_argument(parser: argparse.ArgumentParser, required: bool = True, help: str = "Collection name (will be used as root folder name)") -> None:
    parser.add_argument("-c", "--collection", required=required, help=help)


def add_indexers_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("-i", "--indexers", required=False, default=DEFAULT_INDEXERS, nargs="+", help="List of indexer names")


def add_indexes_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("-i", "--indexes", required=False, default=None, nargs="+",
                        help="Index(es) for search. Multiple can be specified (e.g. --indexes index1 index2). If not specified, all available indexes are used. Multiple indexes are combined using Reciprocal Rank Fusion.")


def add_rrf_k_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("-k", "--rrfK", required=False, type=int, default=60,
                        help="RRF constant for multi-index search fusion. Higher values reduce rank impact.")


def add_filter_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("-l", "--filter", required=False, default=None,
                        help="""Filter query for search. Uses common syntax: 'field operator "value"'. Multiple conditions can be combined with 'and'/'or'. Examples: --filter 'space = "SPACE_KEY"', --filter 'space = "SPACE_KEY" and lastModifiedAt > "2026-01-01"'""")


def add_chunk_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("-s", "--chunkSize", required=False, type=int, default=1000, help="Chunk size for text splitting (default: 1000)")
    parser.add_argument("-o", "--chunkOverlap", required=False, type=int, default=100, help="Chunk overlap for text splitting (default: 100)")


def add_format_argument(parser: argparse.ArgumentParser, default: str = "json_with_indent") -> None:
    parser.add_argument("-f", "--format", required=False, default=default, choices=FORMAT_CHOICES,
                        help=f"Output format for the result (default: {default})")
