# Use Python 3.13 slim image for smaller size
FROM python:3.13-slim

# Install required system dependencies including git
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Create data directory for storing collections and cache
RUN mkdir -p /data/collections /data/caches /data/local_file_input

# Install uv
RUN pip install --no-cache-dir uv

WORKDIR /app

# Clone the documents-vector-search repository
RUN git clone https://github.com/shnax0210/documents-vector-search.git .

# Install dependencies using uv
RUN uv sync

EXPOSE 8000

# Set entrypoint - provide default command that shows usage
CMD ["python", "-c", "print('documents-vector-search ready! Use: docker run -v $(pwd)/data:/app/data documents-vector-search uv run collection_search_cmd_adapter.py --help')"]
