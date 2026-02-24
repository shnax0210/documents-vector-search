from langchain_text_splitters import RecursiveCharacterTextSplitter


class TextSplitter:
    def __init__(self, chunk_size=1000, chunk_overlap=100):
        self.__chunk_size = chunk_size
        self.__chunk_overlap = chunk_overlap
        self.__splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
        )

    def split_text(self, text):
        return self.__splitter.split_text(text)

    def get_details(self):
        return {
            "chunkSize": self.__chunk_size,
            "chunkOverlap": self.__chunk_overlap,
        }
