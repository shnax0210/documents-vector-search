import os
import logging
import json
import datetime
import re
import PyPDF2

DEFAULT_FILE_TYPE = "default"

class FilesDocumentReader:
    def __init__(self, base_path: str, include_patterns=[".*"], exclude_patterns=[], fail_fast: bool = False):
        self.base_path = base_path

        self.include_patterns = include_patterns
        self.exclude_patterns = exclude_patterns
        self.compiled_include_patterns = [re.compile(pattern) for pattern in include_patterns]
        self.compiled_exclude_patterns = [re.compile(pattern) for pattern in exclude_patterns]

        self.fail_fast = fail_fast
        self.file_readers = {
            '.pdf': self.__read_pdf_file,
        }
        self.default_reader = self.__read_text_file

    def read_all_documents(self):
        result_stats = {
            "successfulNonDefaultTypeFiles": [],
            "successfulDefaultTypeFiles": [],
            "errorNonDefaultTypeFiles": [],
            "errorDefaultTypeFiles": []
        }

        for file_path in self.__read_file_pathes():
            file_extension, file_type, file_content, error = self.__read_file(file_path)

            self.__update_result_stats(file_type, file_path, error, result_stats)

            if error:
                if self.fail_fast:
                    raise RuntimeError(f"Error reading file {file_path}") from error

                logging.exception(f"Error reading file {file_path}", error)
                continue
            
            yield {
               "fileRelativePath": os.path.relpath(file_path, self.base_path),
               "fileFullPath": file_path,
               "modifiedTime": self.__read_file_modification_time(file_path),
               "fileType": file_type,
               "fileExtension": file_extension,
               "content": file_content
            }
        
        logging.info(f"Files reading stats: \n{json.dumps(result_stats, indent=4)}")

    def get_number_of_documents(self):
        return len(self.__read_file_pathes())

    def get_reader_details(self) -> dict:
        return {
            "type": "localFiles",
            "basePath": self.base_path,
        }

    def __update_result_stats(self, file_type: str, file_path: str, error, result_stats: dict):
        if error:
            if file_type is DEFAULT_FILE_TYPE:
                result_stats["errorDefaultTypeFiles"].append(file_path)
            else:
                result_stats["errorNonDefaultTypeFiles"].append(file_path)
        else:
            if file_type is DEFAULT_FILE_TYPE:
                result_stats["successfulDefaultTypeFiles"].append(file_path)
            else:
                result_stats["successfulNonDefaultTypeFiles"].append(file_path)
        
    
    def __read_file(self, file_path: str):
        file_extension = os.path.splitext(file_path)[1].lower()
        file_type = file_extension if file_extension in self.file_readers else DEFAULT_FILE_TYPE
        file_reader = self.file_readers.get(file_extension, self.default_reader)

        try:
            return file_extension, file_type, file_reader(file_path), None
        except Exception as e:
            return file_extension, file_type, None, e
        
    def __read_file_modification_time(self, file_path: str):
        mod_time = os.path.getmtime(file_path)
        return datetime.datetime.fromtimestamp(mod_time).isoformat()

    def __read_file_pathes(self):
        return [os.path.join(self.base_path, file_path) for file_path in os.listdir(self.base_path) if
                (os.path.isfile(os.path.join(self.base_path, file_path)) and self.__is_file_included(file_path) and not self.__is_file_excluded(file_path))]
    
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

    def __read_pdf_file(self, file_path: str):
        result = []
        with open(file_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            page_number = 0
            for page in pdf_reader.pages:
                page_number += 1
                result.append({
                    "identifier": {
                        "pageNumber": page_number
                    },
                    "text": page.extract_text()
                })
        return result