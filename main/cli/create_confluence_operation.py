import argparse
import os

from main.cli.base_cli_operation import BaseCliOperation
from main.cli.cli_arguments import add_chunk_arguments, add_collection_argument, add_indexers_argument
from main.factories.create_collection_factory import create_collection_creator
from main.sources.base_document_converter import BaseDocumentConverter
from main.sources.base_document_reader import BaseDocumentReader
from main.sources.confluence.confluence_cloud_document_converter import ConfluenceCloudDocumentConverter
from main.sources.confluence.confluence_cloud_document_reader import ConfluenceCloudDocumentReader
from main.sources.confluence.confluence_document_converter import ConfluenceDocumentConverter
from main.sources.confluence.confluence_document_reader import ConfluenceDocumentReader
from main.splitter.base_text_splitter import BaseTextSplitter
from main.splitter.text_splitter import TextSplitter


class CreateConfluenceOperation(BaseCliOperation):
    def name(self) -> str:
        return "create-confluence"

    def help(self) -> str:
        return "Create a collection from Confluence pages (Server/Data Center or Cloud)"

    def configure(self, parser: argparse.ArgumentParser) -> None:
        add_collection_argument(parser, help="Collection name (will be used as root folder name)")

        parser.add_argument("-u", "--url", required=True,
                            help="Confluence base url (e.g., https://your-domain.atlassian.net for Cloud or https://confluence.example.com for Server/Data Center)")
        parser.add_argument("-q", "--cql", required=False, default="", help="Confluence query (CQL) to get pages for indexing")

        add_indexers_argument(parser)

        parser.add_argument("-r", "--readOnlyFirstLevelComments", action="store_true", required=False, default=False,
                            help="Confluence has hierarchical comments, first level comments are read by default, but for other ones additional call is needed what can slowdown the process. Pass this argument to read only first level comments and have better performance.")

        add_chunk_arguments(parser)

    def run(self, args: dict) -> None:
        text_splitter = TextSplitter(chunk_size=args["chunkSize"], chunk_overlap=args["chunkOverlap"])

        if args["url"].endswith(".atlassian.net"):
            document_reader, document_converter = self.__create_cloud_reader_and_converter(args, text_splitter)
        else:
            document_reader, document_converter = self.__create_server_reader_and_converter(args, text_splitter)

        create_collection_creator(collection_name=args["collection"],
                                  indexers=args["indexers"],
                                  document_reader=document_reader,
                                  document_converter=document_converter).run()

    def __create_cloud_reader_and_converter(self, args: dict, text_splitter: BaseTextSplitter) -> tuple[BaseDocumentReader, BaseDocumentConverter]:
        email = os.environ.get("ATLASSIAN_EMAIL")
        api_token = os.environ.get("ATLASSIAN_TOKEN")

        if not email or not api_token:
            raise ValueError("Both 'ATLASSIAN_EMAIL' and 'ATLASSIAN_TOKEN' environment variables must be provided for Confluence Cloud.")

        document_reader = ConfluenceCloudDocumentReader(base_url=args["url"],
                                                       query=args["cql"],
                                                       email=email,
                                                       api_token=api_token,
                                                       read_all_comments=(not args["readOnlyFirstLevelComments"]))

        return document_reader, ConfluenceCloudDocumentConverter(text_splitter)

    def __create_server_reader_and_converter(self, args: dict, text_splitter: BaseTextSplitter) -> tuple[BaseDocumentReader, BaseDocumentConverter]:
        token = os.environ.get("CONF_TOKEN")
        login = os.environ.get("CONF_LOGIN")
        password = os.environ.get("CONF_PASSWORD")

        if not token and (not login or not password):
            raise ValueError("Either 'token' ('CONF_TOKEN' env variable) or both 'login' ('CONF_LOGIN' env variable) and 'password' ('CONF_PASSWORD' env variable) must be provided.")

        document_reader = ConfluenceDocumentReader(base_url=args["url"],
                                                   query=args["cql"],
                                                   token=token,
                                                   login=login,
                                                   password=password,
                                                   read_all_comments=(not args["readOnlyFirstLevelComments"]))

        return document_reader, ConfluenceDocumentConverter(text_splitter)
