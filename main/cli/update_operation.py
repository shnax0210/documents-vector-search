import argparse

from main.cli.base_cli_operation import BaseCliOperation
from main.cli.cli_arguments import add_collection_argument
from main.factories.update_collection_factory import create_collection_updater


class UpdateOperation(BaseCliOperation):
    def name(self) -> str:
        return "update"

    def help(self) -> str:
        return "Update an existing collection (indexes only new and changed documents)"

    def configure(self, parser: argparse.ArgumentParser) -> None:
        add_collection_argument(parser, help="Collection name (will be used to determine root folder and manifest file)")

    def run(self, args: dict) -> None:
        create_collection_updater(args["collection"]).run()
