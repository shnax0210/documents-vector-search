# Core instructions

## Keep it simple
Start from minimal working solution and iterate only when explicitly asked. Use Occam's razor principle.

## Follow SOLID principles
Pay attention to Open Close. If reasonable use list of objects with same interface, so it's possbile to add new logic to the list without code changes.

## Follow GRASP principles
Pay attention to "Follow High Cohesion and Low Coupling". Reduce dependencies between code/documentation/terminology parts and keep related code/documentation/terminology together.

## Be consistent with existing code/documentation/terminology

# Code instructions

## Use fail fast approach by default
Don't catch exceptions unless it's top level code or you re-raise them with more context.

## Use early returns to reduce nesting

## Don't add comments
- Prefere self-explanatory code and don't add comments unless it's absolutely necessary to describe why the code does something in a specific way;
- Never add comments to describe what the code does, it should be clear from the code itself.

## Use encapsulation intensively
- Make all functions private by default by adding "__" prefix unless the functin is used from another file.
- Don't add the __" prefix for variables;

## Add interface for each class
For each class, add an abstract base class that defines the interface for the class and make the class inherit from it.

## Add parameters and return types to all public functions and methods

## Prefere stateless code
When creating functions, prefer passing parameters instead of using global state.

## Avoid wrapping code in `if __name__ == "__main__":` unless explicitly asked

# Project stack

## Use `uv` for dependency management and code execution
- `uv add <package>`
- `uv run <script>`

# Prefere MCP if it's available for search

When user asks to search something in Confluence/Jira/Files, MCP for collection search is added and there is a relevant collection - always use the MCP (avoid direct search in files or running of `collection_search_cmd_adapter.py` and `collection_fetch_cmd_adapter.py` scripts unless explicitly asked). 