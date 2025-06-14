import os
import logging
import json
import datetime
import re
from unstructured.partition.auto import partition

EXCLUDED_FILE_EXTENSIONS = [
    ".DS_Store",
    # Archive and compressed formats
    ".zip",
    ".tar",
    ".jar",
    ".rar",
    ".gz",
    ".tgz",
    ".7z",
    ".bz2",
    ".xz",
    ".lz4",
    ".zst",
    ".cab",
    ".deb",
    ".rpm",
    ".pkg",
    ".dmg",
    ".iso",
    ".img",
    # Executable and binary formats
    ".exe",
    ".dll",
    ".so",
    ".dylib",
    ".app",
    ".msi",
    ".bin",
    ".run",
    ".deb",
    ".rpm",
    # Compiled code
    ".class",
    ".pyc",
    ".pyo",
    ".o",
    ".obj",
    ".lib",
    ".a",
    ".bundle",
    # Video formats
    ".mp4",
    ".avi",
    ".mkv",
    ".mov",
    ".wmv",
    ".flv",
    ".webm",
    ".m4v",
    ".3gp",
    ".mpg",
    ".mpeg",
    ".ts",
    ".vob",
    ".ogv",
    # Audio formats
    ".mp3",
    ".wav",
    ".flac",
    ".aac",
    ".ogg",
    ".wma",
    ".m4a",
    ".opus",
    ".aiff",
    ".au",
    ".ra",
    # Font formats
    ".ttf",
    ".otf",
    ".woff",
    ".woff2",
    ".eot",
    # Database formats
    ".db",
    ".sqlite",
    ".sqlite3",
    ".mdb",
    ".accdb",
    # Virtual machine and disk images
    ".vmdk",
    ".vdi",
    ".qcow2",
    ".vhd",
    ".vhdx",
    # Other binary formats
    ".swf",
    ".fla",
    ".unity3d",
    ".unitypackage",
    ".blend",
    ".max",
    ".3ds",
    ".fbx",
    ".dae",
    ".obj",
    ".stl",
    ".ply",
]

class FilesDocumentReader:
    def __init__(self, base_path: str, 
                 include_patterns=[".*"], 
                 exclude_patterns=[], 
                 fail_fast: bool = False, 
                 start_from_time = None):
        self.base_path = base_path

        self.include_patterns = include_patterns
        self.exclude_patterns = exclude_patterns
        self.compiled_include_patterns = [re.compile(pattern) for pattern in include_patterns]
        self.compiled_exclude_patterns = [re.compile(pattern) for pattern in exclude_patterns]

        self.fail_fast = fail_fast
        self.start_from_time = start_from_time

        self.file_readers = {
            ".json": self.__read_text_file, # By some reason unstructured lib tries to read json files as ndjson and fails
        }
        self.default_reader = self.__read_file_by_unstructured_lib

    def read_all_documents(self):
        result_stats = {
            "successFiles": [],
            "errorFiles": [],
        }

        for file_path in self.__read_file_pathes():
            file_content, error = self.__read_file(file_path)

            self.__update_result_stats(result_stats, file_path, error)

            if error:
                if self.fail_fast:
                    raise RuntimeError(f"Error reading file {file_path}") from error

                logging.exception(f"Error reading file {file_path}", error)
                continue
            
            yield {
               "fileRelativePath": os.path.relpath(file_path, self.base_path),
               "fileFullPath": file_path,
               "modifiedTime": self.__read_file_modification_time(file_path).isoformat(),
               "content": file_content
            }
        
        logging.info(f"Files reading stats: \n{json.dumps(result_stats, indent=2, ensure_ascii=False)}")

    def get_number_of_documents(self):
        return len(self.__read_file_pathes())

    def get_reader_details(self) -> dict:
        return {
            "type": "localFiles",
            "basePath": self.base_path,
        }  

    def __update_result_stats(self, result_stats, file_path, error):
        if error:
            result_stats["errorFiles"].append(file_path)
        else:
            result_stats["successFiles"].append(file_path)
 
    def __read_file(self, file_path: str):
        file_extension = os.path.splitext(file_path)[1].lower()
        file_reader = self.file_readers.get(file_extension, self.default_reader)

        try:
            return file_reader(file_path), None
        except Exception as e:
            return None, e
        
    def __read_file_modification_time(self, file_path: str):
        mod_time = os.path.getmtime(file_path)
        return datetime.datetime.fromtimestamp(mod_time)

    def __read_file_pathes(self):
        file_paths = []
        for root, _, files in os.walk(self.base_path):
            for file_name in files:
                full_path = os.path.join(root, file_name)
                relative_path = os.path.relpath(full_path, self.base_path)

                if (
                    os.path.isfile(full_path)
                    and self.__is_file_included(relative_path)
                    and not any(relative_path.endswith(ext) for ext in EXCLUDED_FILE_EXTENSIONS)
                    and not self.__is_file_excluded(relative_path)
                    and (self.start_from_time is None or self.__read_file_modification_time(full_path) >= self.start_from_time)
                ):
                    file_paths.append(full_path)

        return file_paths
    
    def __is_file_included(self, file_path: str):
        return any(pattern.fullmatch(file_path) for pattern in self.compiled_include_patterns)
    
    def __is_file_excluded(self, file_path: str):
        return any(pattern.fullmatch(file_path) for pattern in self.compiled_exclude_patterns)

    def __read_text_file(self, file_path: str):
        with open(file_path, 'r') as file:
            file_content = file.read()
            return [
                {
                    "text": file_content
                }
            ]

    def __read_file_by_unstructured_lib(self, file_path: str):
        elements = partition(filename=file_path)

        if not elements:
            logging.warning(f"No text content found in file: {file_path}")
            return []
        
        if elements[0].metadata.page_number is None:
            return [{
                "text": "\n\n".join([element.text for element in elements if hasattr(element, 'text')]).strip(),
            }]

        return [
            {
                "metadata": {
                    "pageNumber": page_number
                }, 
                "text": "\n\n".join(texts).strip()
            } 
            for page_number, texts in self.__groud_by_page_number(elements).items()]
    
    def __groud_by_page_number(self, elements):
        grouped_elements = {}
        for element in elements:
            page_number = element.metadata.page_number
            if page_number not in grouped_elements:
                grouped_elements[page_number] = []

            if hasattr(element, 'text'):
                grouped_elements[page_number].append(element.text)
        
        return grouped_elements
