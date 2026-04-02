import argparse
import logging

from main.utils.formatting import format_object
from main.utils.logger import setup_root_logger
from main.factories.fetch_collection_factory import create_collection_fetcher

setup_root_logger()

ap = argparse.ArgumentParser()
ap.add_argument("-collection", "--collection", required=True, help="Collection name (will be used as root folder name)")

ap.add_argument("-id", "--id", required=True, help="Document ID to fetch")

ap.add_argument("-startLine", "--startLine", required=False, type=int, default=1, help="Start line number (1-based, default: 1)")
ap.add_argument("-endLine", "--endLine", required=False, type=int, default=200, help="End line number (inclusive, default: 200)")

ap.add_argument("-format", "--format", default="json_with_indent", required=False, choices=['json', 'json_with_indent', 'toon'], help="Output format for the result (e.g., 'json', 'json_with_indent', 'toon')")
args = vars(ap.parse_args())

fetcher = create_collection_fetcher(collection_name=args['collection'])
result = fetcher.fetch(id=args['id'], start_line=args['startLine'], end_line=args['endLine'])

logging.info(f"Fetch result:\n{format_object(result, args['format'])}")
