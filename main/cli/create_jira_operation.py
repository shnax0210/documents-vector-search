import argparse
import os

from main.cli.base_cli_operation import BaseCliOperation
from main.cli.cli_arguments import add_chunk_arguments, add_collection_argument, add_indexers_argument
from main.factories.create_collection_factory import create_collection_creator
from main.sources.base_document_converter import BaseDocumentConverter
from main.sources.base_document_reader import BaseDocumentReader
from main.sources.jira.jira_cloud_document_converter import JiraCloudDocumentConverter
from main.sources.jira.jira_cloud_document_reader import JiraCloudDocumentReader
from main.sources.jira.jira_document_converter import JiraDocumentConverter
from main.sources.jira.jira_document_reader import JiraDocumentReader
from main.splitter.base_text_splitter import BaseTextSplitter
from main.splitter.text_splitter import TextSplitter


class CreateJiraOperation(BaseCliOperation):
    def name(self) -> str:
        return "create-jira"

    def help(self) -> str:
        return "Create a collection from Jira tickets (Server/Data Center or Cloud)"

    def configure(self, parser: argparse.ArgumentParser) -> None:
        add_collection_argument(parser, help="Collection name (will be used as root folder name)")

        parser.add_argument("-u", "--url", required=True,
                            help="Jira base url (Cloud: https://your-domain.atlassian.net, Server/Data Center: https://jira.example.com)")
        parser.add_argument("-q", "--jql", required=False, default="", help="Jira query (JQL) to get tickets for indexing")

        add_indexers_argument(parser)
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
            raise ValueError("Both 'ATLASSIAN_EMAIL' and 'ATLASSIAN_TOKEN' environment variables must be provided for Jira Cloud.")

        document_reader = JiraCloudDocumentReader(base_url=args["url"],
                                                 query=args["jql"],
                                                 email=email,
                                                 api_token=api_token)

        return document_reader, JiraCloudDocumentConverter(text_splitter)

    def __create_server_reader_and_converter(self, args: dict, text_splitter: BaseTextSplitter) -> tuple[BaseDocumentReader, BaseDocumentConverter]:
        token = os.environ.get("JIRA_TOKEN")
        login = os.environ.get("JIRA_LOGIN")
        password = os.environ.get("JIRA_PASSWORD")

        if not token and (not login or not password):
            raise ValueError("Either 'token' ('JIRA_TOKEN' env variable) or both 'login' ('JIRA_LOGIN' env variable) and 'password' ('JIRA_PASSWORD' env variable) must be provided for Jira Server/Data Center.")

        document_reader = JiraDocumentReader(base_url=args["url"],
                                             query=args["jql"],
                                             token=token,
                                             login=login,
                                             password=password)

        return document_reader, JiraDocumentConverter(text_splitter)
