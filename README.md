# Project allows document indexing in a local vector database and then search (supports Jira, Confluence and local files, can be integrated via MCP)

- [Base info](#base-info)
- [Common use case](#common-use-case)
- [How to set up and use](#how-to-set-up-and-use)
  - [Create collection for Confluence](#create-collection-for-confluence)
  - [Create collection for Jira](#create-collection-for-jira)
  - [Update existing collection](#update-existing-collection)
  - [Search in collection](#search-in-collection)
  - [Set up MCP](#set-up-mcp)
- [Collection structure](#collection-structure)
- [Other useful info](#other-useful-info)

## Base info

Key points:
- Supports Jira/Confluence Data Center/Server and Cloud. For Jira ticket is a document, for Confluence page is a document.
- Supports local files from a specified folder in various formats like: .pdf, .pptx, .docx, etc. Uses [Unstructured](https://github.com/Unstructured-IO/unstructured) for local files parsing;
- Does NOT send any data to any third-party systems. All data are processed locally and stored locally (except in the case when you use it as MCP with a non-local AI agent).
- Supports MCP protocol to use the vector search as a tool in AI agents.
- Supports "update" operation, so there is no need to fully recreate the vector database each time.
- Provides an abstraction to add more data sources and to use different technologies (embeddings, vector databases, etc.).

Key technologies used:
- "FAISS" lib (https://github.com/facebookresearch/faiss) for vector search;
- "sentence-transformers" lib (https://pypi.org/project/sentence-transformers/) for embeddings;
- "Unstructured" lib: https://github.com/Unstructured-IO/unstructured.
- "LangChain" lib: https://python.langchain.com/docs/introduction/

Please check this article for more context: https://medium.com/@shnax0210/mcp-tool-for-vector-search-in-confluence-and-jira-6beeade658ba

## Common use case
1) You create a collection by a dedicated script (there are separate scripts for Jira, Confluence and local files cases). During the collection creation, data are loaded into your local machine and then indexed. Results are stored in a subfolder of `./data/collections` with the name that you specify via the "--collection ${collectionName}" parameter. So a collection is just a folder with all needed information for search, such as: loaded documents, index files, metadata, etc. Once a collection is created, it can be used for search and update. The creation process can take a while; it depends on the number of documents your collection consists of and local machine resources.
2) After some time, you may want to update existing collections to get new data, you can do it via a dedicated script. You will need to specify the collection name used during collection creation. Collection update reads and indexes only new/updated documents, so it should be much faster than collection creation.
3) You can search in an existing collection by dedicated script.
4) You can set up MCP tool for existing collection, so an AI agent will be able to use the search.

You can create different collections for different use cases. For example, you can create a collection for all Confluence pages to do a general search, and you can create a collection for pages from a specific Confluence space, so you will do a more narrow search.

## How to set up and use

1) Clone the repository
2) Install `uv`: https://docs.astral.sh/uv/
3) Navigate to the root project folder and run: `uv sync`

### Create collection for Confluence:

1) Set env variables needed for authentification/authorization:
- **For Confluence Server/Data Center:** set CONF_TOKEN env variable with your Confluence Bearer token (optionally, you can set CONF_LOGIN and CONF_PASSWORD env variables instead with your Confluence user login and password, but the token variant is more recommended).
- **For Confluence Cloud:** set ATLASSIAN_EMAIL env variable with your Atlassian account email and ATLASSIAN_TOKEN env variable with your Atlassian Cloud API token. (Generate API token at: https://id.atlassian.com/manage/api-tokens)

2) Run command like:
```
uv run confluence_collection_create_cmd_adapter.py --collection "confluence" --url "${baseConfluenceUrl}" --cql "${confluenceQuery}"
```

Notes:
- The script automatically detects whether your Confluence instance is Cloud or Server/Data Center based on the URL:
  - URLs ending with `.atlassian.net` are treated as Confluence Cloud
  - All other URLs are treated as Confluence Server/Data Center
- You can use different values for the "collection" parameter, but you will need to use the same value during collection updates and searches. It defines the collection name, and all collection data will be stored in a folder with that name under `./data/collections`;
- Please update ${baseConfluenceUrl} to the real Confluence base URL:
  - For Server/Data Center, example: https://confluence.example.com
  - For Cloud, example: https://your-domain.atlassian.net
- Please update ${confluenceQuery} to the real Confluence query, for example: "(space = 'MySpaceName') AND (created >= '2025-01-01' OR lastModified >= '2025-01-01')"

### Create collection for Jira:

1) Set env variables needed for authentification/authorization:
- **For Jira Server/Data Center:** set JIRA_TOKEN env variable with your Jira Bearer token (optionally, you can set JIRA_LOGIN and JIRA_PASSWORD env variables instead with your Jira user login and password, but the token variant is more recommended).
- **For Jira Cloud:** set ATLASSIAN_EMAIL env variable with your Atlassian account email and ATLASSIAN_TOKEN env variable with your Atlassian Cloud API token. (Generate API token at: https://id.atlassian.com/manage/api-tokens)

2) Run command like:
```
uv run jira_collection_create_cmd_adapter.py --collection "jira" --url "${baseJiraUrl}" --jql "${jiraQuery}"
```

Notes:
- The script automatically detects whether your Jira instance is Cloud or Server/Data Center based on the URL:
  - URLs ending with `.atlassian.net` are treated as Jira Cloud
  - All other URLs are treated as Jira Server/Data Center
- You can use different values for the "collection" parameter, but you will need to use the same value during collection updates and searches. It defines the collection name, and all collection data will be stored in a folder with that name under `./data/collections`;
- Please update ${baseJiraUrl} to the real Jira base URL:
  - For Server/Data Center, example: https://jira.example.com
  - For Cloud, example: https://your-domain.atlassian.net
- Please update ${jiraQuery} to the real Jira query, for example: "project = MyProjectName AND created >= -183d"

### Create collection for local files


1) Run a command like:
```
uv run files_collection_create_cmd_adapter.py --basePath "${pathToFolderWithFiles}"
```

Notes:
- Please update `${pathToFolderWithFiles}` to the actual folder path.
- By default, the collection will be named after the last folder in `--basePath` (for example, if `--basePath` is "/Users/a/b", the collection name will be "b"). You can override this by adding `--collection ${collectionName}`, as in all other scripts.
- By default, if a file cannot be read, it is just skipped and written to the log. You can override this by adding the `--failFast` parameter, so the script will fail immediately after the first error.
- By default, all files from `${pathToFolderWithFiles}` are included (except for some predefined types, like zip, jar, etc.). You can adjust this by adding `--includePatterns` and `--excludePatterns` parameters with regexes. If you specify both `--includePatterns` and `--excludePatterns`, only files that match `--includePatterns` and do not match `--excludePatterns` will be included. Examples:
    - Example of `--includePatterns` (the parameter can be used multiple times): `--includePatterns "/subfolder1/.*" --includePatterns "/subfolder2/.*"`.
    - Example of `--excludePatterns` (the parameter can be used multiple times): `--excludePatterns "/subfolder1/.*" --excludePatterns "/subfolder2/.*"`.
- The script uses the [Unstructured](https://github.com/Unstructured-IO/unstructured) Python library, which supports many [file formats](https://docs.unstructured.io/welcome#supported-file-types) such as .pdf, .pptx, .docx, etc. Some file formats may require additional software installation, listed [here](https://docs.unstructured.io/open-source/installation/full-installation#full-installation).

### Update existing collection:

1) Set env variables needed for authentification/authorization (not needed for local files):
- **For Confluence Server/Data Center:** set CONF_TOKEN env variable with your Confluence Bearer token (optionally, you can set CONF_LOGIN and CONF_PASSWORD env variables instead with your Confluence user login and password, but the token variant is more recommended).
- **For Confluence Cloud:** set ATLASSIAN_EMAIL env variable with your Atlassian account email and ATLASSIAN_TOKEN env variable with your Atlassian Cloud API token. (Generate API token at: https://id.atlassian.com/manage/api-tokens)
- **For Jira Server/Data Center:** set JIRA_TOKEN env variable with your Jira Bearer token (optionally, you can set JIRA_LOGIN and JIRA_PASSWORD env variables instead with your Jira user login and password, but the token variant is more recommended).
- **For Jira Cloud:** set ATLASSIAN_EMAIL env variable with your Atlassian account email and ATLASSIAN_TOKEN env variable with your Atlassian Cloud API token. (Generate API token at: https://id.atlassian.com/manage/api-tokens)

2) Run command like:
```
uv run collection_update_cmd_adapter.py --collection "${collectionName}"
```

Notes:
- Please update ${collectionName} to the real collection name (the one used during collection creation), for example: "confluence" or "jira".

### Search in collection:

Run command like:
```
uv run collection_search_cmd_adapter.py --collection "${collectionName}" --query "${searchQuery}"
```

Notes:
- Please update ${collectionName} to the real collection name (the one used during collection creation), for example: "confluence" or "jira";
- Please update ${searchQuery} to the text that you would like to search, for example: "How to set up react project locally";
- You can add the "--includeMatchedChunksText" parameter to include matched chunks of a document text in search results.

### Set up MCP:

Add MCP configuration like:
```
{
    "servers": {
        ...
        "search_${collectionName}_stdio": {
            "type": "stdio",
            "command": "uv",
            "args": [
                "--directory",
                "${fullPathToRootProjectFolder}",
                "run",
                "collection_search_mcp_stdio_adapter.py",
                "--collection",
                "${collectionName}",
            ]
        },
        ...
    }
}
```

If you use VS code IDE and GitHub Copilot, you can add the configuration into `.vscode/mcp.json` file in the root of your project.
You can check more details on youtube:
- https://www.youtube.com/watch?v=VePxCcF99w4
- https://www.youtube.com/watch?v=iS25RFups4A

Notes:
- Please update ${collectionName} to the real collection name (the one used during collection creation), for example: "confluence" or "jira".
- Please update ${fullPathToRootProjectFolder} to the real full path to this project root folder.
- It can be useful to increase the number of returned matched text chunks by setting "--maxNumberOfChunks ${number}". A bigger number means better search, but too large a number may break GitHub Copilot, probably because it does not fit into the model context window.

Prompt examples:
- "Find information about AI use cases, search info on Confluence, include all used links in response"
- "Find information about PDP carousel, search info on Jira, include all used links in response"


## Collection structure
Collection is a subfolder of the `./data/collections` folder.
A collection folder contains all files needed for performing vector search in the collection.

A collection folder consists of:
- `documents` folder contains documents read by `reader` from the `./main/sources` package and converted by `converter` from the `./main/sources` package.
- `indexes` folder contains available indexes (usually just one index but multiple are also supported);
- `manifest.json` file contains information about the index such as name, last update time, reader details, and indexes.

Please check the `./main/core/documents_collection_creator.py` code to find most of the details about collection creation or updating.

Please check the `./main/core/documents_collection_searcher.py` code to find most of the details about searching in a collection.

## Other useful info
- Collection update reads only new information, so it should be much faster than collection creation. Collection update uses information from the collection manifest file located in `./data/collections/${collectionName}/manifest.json`.
- Collection update usually reads a bit more documents than were really updated since last time. Currently, the logic is as follows: it reads all documents that were created/updated since the "lastModifiedDocumentTime" field value from the `./data/collections/${collectionName}/manifest.json` file minus 1 day. It's done so to guarantee that no document update will be lost due to parallel document creations (probably 1 day can be updated to some much less value like a couple of seconds, but it does not look like a big deal to me and I prefer just to be more sure that everything is updated). The "lastModifiedDocumentTime" field contains the value of the latest update time for all documents in the collection.
- There is a cache mechanism for Jira/Confluence collection creation, so if you create a collection multiple times with the same parameters: url, query (JQL or CQL), etc. - documents will be read from the cache located in the `./data/caches` subfolder (all important parameters are collected together and hashed, the hash is used as the folder name (`./data/caches/{hash}`) for cached documents, there is also a `./data/caches/{hash}_completed` file that indicates if all documents were successfully read, the cache is used only in case if the `./data/caches/{hash}_completed` file is present as well as the `./data/caches/{hash}` folder). The cache is useful during testing, but can lead to a situation where new data are not read. In such a case, you can either run the "update" script after collection creation, or remove the cache manually before collection creation.
