from langchain_text_splitters import RecursiveCharacterTextSplitter

class JiraCloudDocumentConverter:
    def __init__(self):
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000,
            chunk_overlap=100,
        )

    def convert(self, document):
        return [{
            "id": document['key'],
            "url": self.__build_url(document),
            "modifiedTime": document['fields']['updated'],
            "metadata": self.__build_metadata(document),
            "text": self.__build_document_text(document),
            "chunks": self.__split_to_chunks(document)
        }]

    def __build_metadata(self, document):
        return {
            "project": document['key'].split("-")[0]
        }
    
    def __build_document_text(self, document):
        main_info = self.__build_main_ticket_info(document)
        description_and_comments = self.__fetch_description_and_comments(document)

        return self.__convert_to_text([main_info, description_and_comments])

    def __split_to_chunks(self, document):
        chunks = [{
                "indexedData": self.__build_main_ticket_info(document)
            }]
        
        description_and_comments = self.__fetch_description_and_comments(document)
        if description_and_comments:
            for chunk in self.text_splitter.split_text(description_and_comments):
                chunks.append({
                    "indexedData": chunk
                })
        
            
        return chunks

    def __fetch_description_and_comments(self, document):
        description = self.__fetch_description(document)
        comments = [self.__convert_content_text(comment['body']) for comment in document['fields']['comment']['comments']]

        return self.__convert_to_text([description] + comments)

    def __fetch_description(self, document):
        description = document['fields']['description']
        if not description:
            return ""
        
        return self.__convert_content_text(description)

    def __convert_content_text(self, field_with_content):
        texts = []

        for content in field_with_content["content"]:
            if "content" in content:
                for content_of_content in content["content"]:
                    if "text" in content_of_content:
                        texts.append(content_of_content["text"])

        return self.__convert_to_text(texts, delimiter="\n")

    def __build_main_ticket_info(self, document):
        return f"{document['key']} : {document['fields']['summary']}"

    def __convert_to_text(self, elements, delimiter="\n\n"):
        return delimiter.join([element for element in elements if element]).strip()
    
    def __build_url(self, document):
        base_url = document['self'].split("/rest/api/")[0]
        return f"{base_url}/browse/{document['key']}" 