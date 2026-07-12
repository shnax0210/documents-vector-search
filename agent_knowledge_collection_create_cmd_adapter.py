#!/usr/bin/env python3
import argparse

from main.factories.create_collection_factory import create_collection_creator
from main.sources.agent_knowledge.agent_knowledge_document_converter import AgentKnowledgeDocumentConverter
from main.sources.agent_knowledge.agent_knowledge_document_reader import AgentKnowledgeDocumentReader
from main.splitter.text_splitter import TextSplitter
from main.utils.logger import setup_root_logger

setup_root_logger()

ap = argparse.ArgumentParser()
ap.add_argument('-collection', '--collection', required=False, default='agent-knowledge', help='Collection name for indexed agent memories and skills.')
ap.add_argument('-projectRoot', '--projectRoot', required=True, help='Project root used to discover .deepagents and .agents folders.')
ap.add_argument('-agentName', '--agentName', required=False, default='agent', help='Agent name used for ~/.deepagents/{agentName}.')
ap.add_argument('-includeUserKnowledge', '--includeUserKnowledge', action=argparse.BooleanOptionalAction, required=False, default=True, help='Include user-level ~/.deepagents/{agentName} and ~/.agents knowledge.')
ap.add_argument('-includeProjectKnowledge', '--includeProjectKnowledge', action=argparse.BooleanOptionalAction, required=False, default=True, help='Include project-level .deepagents and .agents knowledge.')
ap.add_argument('-indexers', '--indexers', required=False, default=['indexer_ChromaDb__embeddings_sentence-transformers_slash_all-MiniLM-L6-v2', 'indexer_SqlLiteBM25'], help='List of indexer names', nargs='+')
ap.add_argument('-failFast', '--failFast', action='store_true', required=False, default=False, help='Stop on first unreadable agent knowledge file.')
ap.add_argument('-chunkSize', '--chunkSize', required=False, default=1000, type=int, help='Chunk size for text splitting (default: 1000)')
ap.add_argument('-chunkOverlap', '--chunkOverlap', required=False, default=100, type=int, help='Chunk overlap for text splitting (default: 100)')
args = vars(ap.parse_args())

text_splitter = TextSplitter(chunk_size=args['chunkSize'], chunk_overlap=args['chunkOverlap'])
reader = AgentKnowledgeDocumentReader(
    project_root=args['projectRoot'],
    agent_name=args['agentName'],
    include_user_knowledge=args['includeUserKnowledge'],
    include_project_knowledge=args['includeProjectKnowledge'],
    fail_fast=args['failFast'],
)
converter = AgentKnowledgeDocumentConverter(text_splitter)
creator = create_collection_creator(
    collection_name=args['collection'],
    indexers=args['indexers'],
    document_reader=reader,
    document_converter=converter,
    use_cache=False,
)
creator.run()
