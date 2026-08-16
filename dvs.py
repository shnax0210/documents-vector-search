import argparse

from main.cli.cli_operations import build_cli_operations
from main.utils.logger import setup_root_logger

operations = build_cli_operations()

ap = argparse.ArgumentParser(prog="dvs.py", description="Local vector search for Jira, Confluence and files")
subparsers = ap.add_subparsers(dest="operation", required=True, metavar="operation")

for operation in operations:
    operation.configure(subparsers.add_parser(operation.name(), help=operation.help(), description=operation.help()))

args = vars(ap.parse_args())

selected_operation = next(operation for operation in operations if operation.name() == args["operation"])

setup_root_logger(use_stderr=selected_operation.log_to_stderr(args))

selected_operation.run(args)
