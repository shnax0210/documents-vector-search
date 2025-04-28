from sentence_transformers import SentenceTransformer

class SentenceEmbedder:
    def __init__(self, model_name="sentence-transformers/all-MiniLM-L6-v2"):
        self.model_name = model_name
        self.model = SentenceTransformer(model_name)

    def embed(self, text):
        return self.model.encode(text)
    
    def get_number_of_dimensions(self):
        return self.model.get_sentence_embedding_dimension()
