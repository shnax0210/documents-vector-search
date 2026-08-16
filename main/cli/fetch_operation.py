import argparse
import logging

from main.cli.base_cli_operation import BaseCliOperation
from main.cli.cli_arguments import add_collection_argument, add_format_argument
from main.factories.fetch_collection_factory import create_collection_fetcher
from main.utils.formatting import format_object


class FetchOperation(BaseCliOperation):
    def name(self) -> str:
        return "fetch"

    def help(self) -> str:
        return "Fetch a document content from a collection by its id"

    def configure(self, parser: argparse.ArgumentParser) -> None:
        add_collection_argument(parser)

        parser.add_argument("-i", "--id", required=True, help="Document ID to fetch")

        parser.add_argument("-s", "--startLine", required=False, type=int, default=1, help="Start line number (1-based, default: 1)")
        parser.add_argument("-e", "--endLine", required=False, type=int, default=200, help="End line number (inclusive, default: 200)")

        add_format_argument(parser)

    def run(self, args: dict) -> None:
        fetcher = create_collection_fetcher(collection_name=args["collection"])
        result = fetcher.fetch(id=args["id"], start_line=args["startLine"], end_line=args["endLine"])

        logging.info(f"Fetch result:\n{format_object(result, args['format'])}")
