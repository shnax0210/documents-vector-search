import logging
import os

from main.sources.base_document_converter import BaseDocumentConverter
from main.sources.agent_knowledge.skill_frontmatter import parse_skill_markdown
from main.splitter.base_text_splitter import BaseTextSplitter


class AgentKnowledgeDocumentConverter(BaseDocumentConverter):
    def __init__(self, text_splitter: BaseTextSplitter):
        self.__text_splitter = text_splitter

    def get_details(self) -> dict:
        return {'splitter': self.__text_splitter.get_details()}

    def convert(self, document) -> list[dict]:
        skill_info = self.__parse_skill(document) if document['knowledgeType'] == 'skill' else None
        metadata = self.__build_metadata(document, skill_info)
        text = self.__build_document_text(document, metadata)
        return [{
            'id': self.__build_document_id(document, metadata),
            'url': self.__build_url(document),
            'metadata': metadata,
            'text': text,
            'chunks': self.__split_to_chunks(document, metadata, text),
        }]

    def __parse_skill(self, document):
        parsed = parse_skill_markdown(document['content'])
        for warning in parsed['warnings']:
            logging.warning(f"{warning}: {document['path']}")
        return parsed

    def __build_metadata(self, document, skill_info):
        frontmatter = skill_info['frontmatter'] if skill_info else {}
        metadata = {
            'knowledgeType': document['knowledgeType'],
            'scope': document['scope'],
            'agentName': document['agentName'],
            'sourcePath': document['path'],
            'rootName': document['rootName'],
            'rootId': document['rootId'],
            'relativePath': document['relativePath'],
            'createdAt': document['createdTime'],
            'lastModifiedAt': document['modifiedTime'],
        }
        if document['knowledgeType'] == 'skill':
            skill_name = str(frontmatter.get('name') or self.__derive_skill_name(document)).strip()
            skill_description = str(frontmatter.get('description') or '').strip()
            metadata.update({'skillName': skill_name, 'skillDescription': skill_description})
            for field in ['license', 'compatibility', 'allowed-tools']:
                if frontmatter.get(field):
                    metadata[self.__metadata_key(field)] = str(frontmatter[field])
            if isinstance(frontmatter.get('metadata'), dict):
                for key, value in frontmatter['metadata'].items():
                    metadata[f'skillMetadata_{key}'] = str(value)
        return metadata

    def __split_to_chunks(self, document, metadata, text):
        chunks = [{
            'metadata': {'chunkType': 'discovery'},
            'indexedData': self.__build_discovery_text(document, metadata),
        }]
        for chunk in self.__text_splitter.split_text(text):
            chunks.append({'metadata': {'chunkType': 'content'}, 'indexedData': chunk})
        return chunks

    def __build_discovery_text(self, document, metadata):
        if document['knowledgeType'] == 'skill':
            return self.__convert_to_text([
                f"Skill: {metadata.get('skillName', '')}",
                f"Description: {metadata.get('skillDescription', '')}",
                f"Scope: {metadata['scope']}",
                f"Path: {metadata['sourcePath']}",
            ], '\n')
        return self.__convert_to_text([
            'Memory',
            f"Scope: {metadata['scope']}",
            f"Path: {metadata['sourcePath']}",
            document['relativePath'],
        ], '\n')

    def __build_document_text(self, document, metadata):
        if document['knowledgeType'] == 'skill':
            return self.__convert_to_text([self.__build_discovery_text(document, metadata), document['content']])
        return self.__convert_to_text([
            f"Memory file: {document['relativePath']}",
            f"Scope: {document['scope']}",
            document['content'],
        ])

    def __build_document_id(self, document, metadata):
        if document['knowledgeType'] == 'skill':
            skill_name = metadata.get('skillName') or self.__derive_skill_name(document)
            return f"skill/{document['scope']}/{skill_name}/SKILL.md"
        return f"memory/{document['scope']}/{document['rootId']}/{document['relativePath']}"

    def __derive_skill_name(self, document):
        parts = document['relativePath'].split('/')
        if len(parts) >= 2 and parts[0] == 'skills':
            return parts[1]
        return os.path.basename(os.path.dirname(document['path']))

    def __build_url(self, document):
        return f"file://{document['path']}"

    def __convert_to_text(self, elements, delimiter='\n\n'):
        return delimiter.join([str(element) for element in elements if element]).strip()

    def __metadata_key(self, field):
        parts = field.split('-')
        return parts[0] + ''.join(part.capitalize() for part in parts[1:])
