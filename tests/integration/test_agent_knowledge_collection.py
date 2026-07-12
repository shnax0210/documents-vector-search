import os
from datetime import datetime, timedelta

from main.core.documents_collection_fetcher import DocumentCollectionFetcher
from main.factories.create_collection_factory import create_collection_creator
from main.factories.search_collection_factory import create_collection_searcher
from main.factories.update_collection_factory import create_collection_updater
from main.persisters.disk_persister import DiskPersister
from main.sources.agent_knowledge.agent_knowledge_document_converter import AgentKnowledgeDocumentConverter
from main.sources.agent_knowledge.agent_knowledge_document_reader import AgentKnowledgeDocumentReader
from main.splitter.text_splitter import TextSplitter


def _write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding='utf-8')


def test_agent_knowledge_collection_create_search_fetch_and_update(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    project_root = tmp_path / 'repo'
    skill_path = project_root / '.agents' / 'skills' / 'triage' / 'SKILL.md'
    _write(project_root / '.deepagents' / 'AGENTS.md', 'Always prefer local private indexes for project context.')
    _write(skill_path, '---\nname: triage\ndescription: Triage bug reports and duplicates\n---\nUse this for defect intake.')

    splitter = TextSplitter(chunk_size=200, chunk_overlap=20)
    creator = create_collection_creator(
        collection_name='agent-knowledge-test',
        indexers=['indexer_SqlLiteBM25'],
        document_reader=AgentKnowledgeDocumentReader(str(project_root), include_user_knowledge=False),
        document_converter=AgentKnowledgeDocumentConverter(splitter),
        use_cache=False,
    )
    creator.run()

    searcher = create_collection_searcher('agent-knowledge-test')
    skill_results = searcher.search('bug duplicates', filter='knowledgeType = "skill"', include_matched_chunks_content=True)
    assert skill_results['results'][0]['metadata']['skillName'] == 'triage'

    fetcher = DocumentCollectionFetcher('agent-knowledge-test', DiskPersister('./data/collections'))
    fetched = fetcher.fetch('skill/project/triage/SKILL.md')
    assert 'defect intake' in fetched['text']

    _write(skill_path, '---\nname: triage\ndescription: Triage escalations\n---\nHandle sevzero incidents.')
    future = datetime.now() + timedelta(days=1)
    os.utime(skill_path, (future.timestamp(), future.timestamp()))

    updater = create_collection_updater('agent-knowledge-test')
    updater.run()

    updated_results = create_collection_searcher('agent-knowledge-test').search('sevzero', filter='knowledgeType = "skill"', include_matched_chunks_content=True)
    assert updated_results['results'][0]['metadata']['skillDescription'] == 'Triage escalations'
