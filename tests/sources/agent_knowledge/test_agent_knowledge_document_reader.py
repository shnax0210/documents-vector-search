from datetime import datetime, timedelta

from main.sources.agent_knowledge.agent_knowledge_document_reader import AgentKnowledgeDocumentReader


def _write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding='utf-8')


def test_reader_discovers_project_memories_and_skills(tmp_path):
    _write(tmp_path / '.deepagents' / 'AGENTS.md', 'Project guidance')
    _write(tmp_path / '.deepagents' / 'memories' / 'api.md', 'API memory')
    _write(tmp_path / '.agents' / 'skills' / 'review' / 'SKILL.md', '---\nname: review\ndescription: Review code\n---\nBody')
    _write(tmp_path / '.agents' / '.hidden' / 'ignored.md', 'Ignore me')

    reader = AgentKnowledgeDocumentReader(str(tmp_path), include_user_knowledge=False)
    docs = list(reader.read_all_documents())

    assert [doc['knowledgeType'] for doc in docs].count('memory') == 2
    assert [doc['knowledgeType'] for doc in docs].count('skill') == 1
    assert {doc['relativePath'] for doc in docs} == {'AGENTS.md', 'memories/api.md', 'skills/review/SKILL.md'}


def test_reader_can_exclude_project_knowledge(tmp_path):
    _write(tmp_path / '.deepagents' / 'AGENTS.md', 'Project guidance')

    reader = AgentKnowledgeDocumentReader(str(tmp_path), include_project_knowledge=False, include_user_knowledge=False)

    assert reader.get_number_of_documents() == 0
    assert list(reader.read_all_documents()) == []


def test_reader_respects_update_watermark(tmp_path):
    _write(tmp_path / '.deepagents' / 'AGENTS.md', 'Project guidance')
    future = datetime.now() + timedelta(days=1)

    reader = AgentKnowledgeDocumentReader(str(tmp_path), include_user_knowledge=False, start_from_time=future)

    assert reader.get_number_of_documents() == 0
