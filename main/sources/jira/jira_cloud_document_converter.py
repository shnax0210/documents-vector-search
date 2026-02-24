class JiraCloudDocumentConverter:
    def __init__(self, text_splitter):
        self.__text_splitter = text_splitter

    def get_details(self):
        return {
            "splitter": self.__text_splitter.get_details(),
        }

    def convert(self, document):
        return [{
            "id": document['key'],
            "url": self.__build_url(document),
            "metadata": {
                "createdAt": document['fields']['created'],
                "createdBy": self.__get_reporter(document),
                "lastModifiedAt": document['fields']['updated'],
                "project": document['key'].split("-")[0],
                "type": self.__get_type(document),
                "epic": self.__get_epic(document),
                "priority": self.__get_priority(document),
                "assignee": self.__get_assignee(document),
                "status": self.__get_status(document),
            },
            "text": self.__build_document_text(document),
            "chunks": self.__split_to_chunks(document)
        }]
    
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
            for chunk in self.__text_splitter.split_text(description_and_comments):
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
    
    def __get_epic(self, document):
        epic = document['fields'].get('epic')
        if epic:
            return epic.get('key')
        parent = document['fields'].get('parent')
        if parent:
            return parent.get('key')
        return None
    
    def __get_status(self, document):
        status = document['fields'].get('status')
        return status.get('name') if status else None
    
    def __get_priority(self, document):
        priority = document['fields'].get('priority')
        return priority.get('name') if priority else None
    
    def __get_assignee(self, document):
        assignee = document['fields'].get('assignee')
        if assignee:
            email = assignee.get('emailAddress')
            return email.lower() if email else None
        return None
    
    def __get_reporter(self, document):
        reporter = document['fields'].get('reporter')
        if reporter:
            email = reporter.get('emailAddress')
            return email.lower() if email else None
        return None
    
    def __get_type(self, document):
        issue_type = document['fields'].get('issuetype')
        return issue_type.get('name') if issue_type else None
    
    def __build_url(self, document):
        base_url = document['self'].split("/rest/api/")[0]
        return f"{base_url}/browse/{document['key']}" 