from main.cli.base_cli_operation import BaseCliOperation
from main.cli.create_confluence_operation import CreateConfluenceOperation
from main.cli.create_files_operation import CreateFilesOperation
from main.cli.create_jira_operation import CreateJiraOperation
from main.cli.fetch_operation import FetchOperation
from main.cli.mcp_operation import McpOperation
from main.cli.mcp_single_operation import McpSingleOperation
from main.cli.search_operation import SearchOperation
from main.cli.update_operation import UpdateOperation


def build_cli_operations() -> list[BaseCliOperation]:
    return [
        CreateConfluenceOperation(),
        CreateJiraOperation(),
        CreateFilesOperation(),
        UpdateOperation(),
        SearchOperation(),
        FetchOperation(),
        McpOperation(),
        McpSingleOperation(),
    ]
