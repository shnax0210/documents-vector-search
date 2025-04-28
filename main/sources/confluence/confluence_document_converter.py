import os

from bs4 import BeautifulSoup
from langchain.text_splitter import RecursiveCharacterTextSplitter

class ConfluenceDocumentConverter:
    def __init__(self):
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000,
            chunk_overlap=100,
        )

    def convert(self, document):
        return [{
            "id": document["page"]['id'],
            "url": self.__build_url(document["page"]),
            "modifiedTime": document["page"]['version']['when'],
            "text": self.__build_document_text(document),
            "chunks": self.__split_to_chunks(document)
        }]
    
    def __build_document_text(self, document):
        title = self.__build_path_of_titles(document["page"])
        body_and_comments = self.__fetch_body_and_comments(document)

        return self.__convert_to_text([title, body_and_comments])

    def __split_to_chunks(self, document):
        chunks = [{
                "indexedData": self.__build_path_of_titles(document["page"]),
            }]
        
        body_and_comments = self.__fetch_body_and_comments(document)
        
        if body_and_comments:
            for chunk in self.text_splitter.split_text(body_and_comments):
                chunks.append({
                    "indexedData": chunk
                })
            
        return chunks
    
    def __fetch_body_and_comments(self, document):
        body = self.__get_cleaned_body(document["page"])
        comments = [self.__get_cleaned_body(comment) for comment in document["comments"]]

        return self.__convert_to_text([body] + comments)

    def __convert_to_text(self, elements, delimiter="\n\n"):
        return delimiter.join([element for element in elements if element])

    def __get_cleaned_body(self, document):
        document_text_html = document["body"]["storage"]["value"]
        if not document_text_html:
            return ""
        
        soup = BeautifulSoup(document_text_html, "html.parser")
        return soup.get_text(separator=os.linesep, strip=True) 

    def __build_path_of_titles(self, document):
        return " -> ".join([ ancestor["title"] for ancestor in document['ancestors'] if "title" in ancestor ] + [document['title']])
    
    def __build_url(self, page):
        base_url = page['_links']['self'].split("/rest/api/")[0]
        return f"{base_url}{page['_links']['webui']}"