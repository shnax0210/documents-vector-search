from main.sources.agent_knowledge.agent_knowledge_document_converter import AgentKnowledgeDocumentConverter


class FakeSplitter:
    def split_text(self, text):
        return [text]

    def get_details(self):
        return {'chunkSize': 1000, 'chunkOverlap': 0}


def test_converter_outputs_skill_metadata_and_discovery_chunk():
    converter = AgentKnowledgeDocumentConverter(FakeSplitter())
    [doc] = converter.convert({
        'knowledgeType': 'skill',
        'scope': 'project',
        'agentName': 'agent',
        'path': '/repo/.agents/skills/review/SKILL.md',
        'rootName': '.agents',
        'rootId': '.agents',
        'relativePath': 'skills/review/SKILL.md',
        'createdTime': '2026-01-01T00:00:00',
        'modifiedTime': '2026-01-02T00:00:00',
        'content': '---\nname: review\ndescription: Review code changes\nmetadata:\n  owner: platform\n---\n# Review\nDo it.',
    })

    assert doc['id'] == 'skill/project/review/SKILL.md'
    assert doc['metadata']['knowledgeType'] == 'skill'
    assert doc['metadata']['skillName'] == 'review'
    assert doc['metadata']['skillDescription'] == 'Review code changes'
    assert doc['metadata']['skillMetadata_owner'] == 'platform'
    assert doc['metadata']['lastModifiedAt'] == '2026-01-02T00:00:00'
    assert doc['chunks'][0]['metadata']['chunkType'] == 'discovery'
    assert 'Skill: review' in doc['chunks'][0]['indexedData']


def test_converter_outputs_memory_id_and_metadata():
    converter = AgentKnowledgeDocumentConverter(FakeSplitter())
    [doc] = converter.convert({
        'knowledgeType': 'memory',
        'scope': 'project',
        'agentName': 'agent',
        'path': '/repo/.deepagents/AGENTS.md',
        'rootName': '.deepagents',
        'rootId': '.deepagents',
        'relativePath': 'AGENTS.md',
        'createdTime': '2026-01-01T00:00:00',
        'modifiedTime': '2026-01-02T00:00:00',
        'content': 'Use snake_case APIs.',
    })

    assert doc['id'] == 'memory/project/.deepagents/AGENTS.md'
    assert doc['metadata']['knowledgeType'] == 'memory'
    assert doc['chunks'][0]['metadata']['chunkType'] == 'discovery'
    assert 'Use snake_case APIs.' in doc['text']
