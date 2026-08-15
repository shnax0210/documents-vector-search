import argparse
import os

from main.cli.base_cli_operation import BaseCliOperation
from main.cli.cli_arguments import add_chunk_arguments, add_collection_argument, add_indexers_argument
from main.factories.create_collection_factory import create_collection_creator
from main.sources.files.files_document_converter import FilesDocumentConverter
from main.sources.files.files_document_reader import FilesDocumentReader
from main.splitter.text_splitter import TextSplitter


class CreateFilesOperation(BaseCliOperation):
    def name(self) -> str:
        return "create-files"

    def help(self) -> str:
        return "Create a collection from local files"

    def configure(self, parser: argparse.ArgumentParser) -> None:
        add_collection_argument(parser, required=False,
                                help="Collection name (will be used as root folder name). If not provided, it will be derived from the basePath folder name.")

        parser.add_argument("-p", "--basePath", required=True, help="Path to the root folder from which files will be read.")
        parser.add_argument("-n", "--includePatterns", required=False, default=[".*"], nargs="+", help="List of file patterns to include into collection")
        parser.add_argument("-x", "--excludePatterns", required=False, default=[], nargs="+", help="List of file patterns to NOT include into collection")

        add_indexers_argument(parser)

        parser.add_argument("-e", "--failFast", action="store_true", required=False, default=False,
                            help="If passed - the process will stop on the first error. Otherwise, it will try to process all files and log errors for those that failed.")

        add_chunk_arguments(parser)

    def run(self, args: dict) -> None:
        text_splitter = TextSplitter(chunk_size=args["chunkSize"], chunk_overlap=args["chunkOverlap"])

        document_reader = FilesDocumentReader(base_path=args["basePath"],
                                              include_patterns=args["includePatterns"],
                                              exclude_patterns=args["excludePatterns"],
                                              fail_fast=args["failFast"])

        collection_name = args["collection"] if args["collection"] else os.path.basename(args["basePath"])

        create_collection_creator(collection_name=collection_name,
                                  indexers=args["indexers"],
                                  document_reader=document_reader,
                                  document_converter=FilesDocumentConverter(text_splitter),
                                  use_cache=False).run()
