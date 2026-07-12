import datetime
import logging
import os
from typing import Generator

from main.sources.base_document_reader import BaseDocumentReader


class AgentKnowledgeDocumentReader(BaseDocumentReader):
    def __init__(self,
                 project_root: str,
                 agent_name: str = 'agent',
                 include_user_knowledge: bool = True,
                 include_project_knowledge: bool = True,
                 fail_fast: bool = False,
                 start_from_time=None):
        self.project_root = os.path.abspath(os.path.expanduser(project_root))
        self.agent_name = agent_name
        self.include_user_knowledge = include_user_knowledge
        self.include_project_knowledge = include_project_knowledge
        self.fail_fast = fail_fast
        self.start_from_time = start_from_time
        self._documents_cache = None

    def read_all_documents(self) -> Generator:
        for candidate in self.__discover_documents():
            try:
                with open(candidate['path'], 'r', encoding='utf-8') as f:
                    content = f.read()
                yield {
                    **candidate,
                    'createdTime': self.__read_file_creation_time(candidate['path']).isoformat(timespec='seconds'),
                    'modifiedTime': self.__read_file_modification_time(candidate['path']).isoformat(timespec='seconds'),
                    'content': content,
                }
            except Exception as e:
                if self.fail_fast:
                    raise RuntimeError(f"Error reading agent knowledge file {candidate['path']}") from e
                logging.exception(f"Error reading agent knowledge file {candidate['path']}")

    def get_number_of_documents(self) -> int:
        return len(self.__discover_documents())

    def get_reader_details(self) -> dict:
        return {
            'type': 'agentKnowledge',
            'projectRoot': self.project_root,
            'agentName': self.agent_name,
            'includeUserKnowledge': self.include_user_knowledge,
            'includeProjectKnowledge': self.include_project_knowledge,
            'failFast': self.fail_fast,
        }

    def __discover_documents(self) -> list[dict]:
        if self._documents_cache is not None:
            return self._documents_cache
        documents = []
        for root in self.__iter_roots():
            if not os.path.isdir(root['path']):
                continue
            documents.extend(self.__discover_memory_documents(root))
            documents.extend(self.__discover_skill_documents(root))
        self._documents_cache = documents
        return documents

    def __iter_roots(self):
        if self.include_project_knowledge:
            yield {'path': os.path.join(self.project_root, '.deepagents'), 'scope': 'project', 'rootName': '.deepagents', 'rootId': '.deepagents'}
            yield {'path': os.path.join(self.project_root, '.agents'), 'scope': 'project', 'rootName': '.agents', 'rootId': '.agents'}
        if self.include_user_knowledge:
            home = os.path.expanduser('~')
            yield {'path': os.path.join(home, '.deepagents', self.agent_name), 'scope': 'user', 'rootName': f'~/.deepagents/{self.agent_name}', 'rootId': f'.deepagents/{self.agent_name}'}
            yield {'path': os.path.join(home, '.agents'), 'scope': 'user', 'rootName': '~/.agents', 'rootId': '.agents'}

    def __discover_memory_documents(self, root: dict) -> list[dict]:
        result = []
        for current_root, dirnames, filenames in os.walk(root['path']):
            dirnames[:] = [d for d in dirnames if self.__should_descend(d)]
            for filename in filenames:
                full_path = os.path.join(current_root, filename)
                relative_path = os.path.relpath(full_path, root['path'])
                if self.__is_skill_file(relative_path) or not self.__is_memory_file(relative_path):
                    continue
                if not self.__is_modified_after_watermark(full_path):
                    continue
                result.append(self.__build_candidate(root, full_path, relative_path, 'memory'))
        return result

    def __discover_skill_documents(self, root: dict) -> list[dict]:
        skills_root = os.path.join(root['path'], 'skills')
        if not os.path.isdir(skills_root):
            return []
        result = []
        for current_root, dirnames, filenames in os.walk(skills_root):
            dirnames[:] = [d for d in dirnames if not self.__is_ignored_directory(d)]
            for filename in filenames:
                if filename != 'SKILL.md':
                    continue
                full_path = os.path.join(current_root, filename)
                if not self.__is_modified_after_watermark(full_path):
                    continue
                relative_path = os.path.relpath(full_path, root['path'])
                result.append(self.__build_candidate(root, full_path, relative_path, 'skill'))
        return result

    def __build_candidate(self, root: dict, path: str, relative_path: str, knowledge_type: str) -> dict:
        return {
            'path': path,
            'relativePath': relative_path.replace(os.sep, '/'),
            'rootPath': root['path'],
            'rootName': root['rootName'],
            'rootId': root['rootId'],
            'scope': root['scope'],
            'knowledgeType': knowledge_type,
            'agentName': self.agent_name,
        }

    def __should_descend(self, dirname: str) -> bool:
        if dirname in {'skills', 'memories'}:
            return dirname == 'memories'
        return not self.__is_ignored_directory(dirname)

    def __is_memory_file(self, relative_path: str) -> bool:
        normalized = relative_path.replace(os.sep, '/')
        return normalized == 'AGENTS.md' or normalized.endswith('.md')

    def __is_skill_file(self, relative_path: str) -> bool:
        normalized = relative_path.replace(os.sep, '/')
        return normalized.startswith('skills/') and normalized.endswith('/SKILL.md')

    def __is_ignored_directory(self, dirname: str) -> bool:
        return dirname.startswith('.') or dirname in {'__pycache__', 'node_modules', '.pytest_cache', '.mypy_cache'}

    def __is_modified_after_watermark(self, file_path: str) -> bool:
        if self.start_from_time is None:
            return True
        return self.__read_file_modification_time(file_path) > self.start_from_time

    def __read_file_modification_time(self, file_path: str):
        return datetime.datetime.fromtimestamp(os.path.getmtime(file_path))

    def __read_file_creation_time(self, file_path: str):
        return datetime.datetime.fromtimestamp(os.path.getctime(file_path))
