# Core instructions

## Keep it simple
Start from minimal working solution and iterate only when explicitly asked.

## Follow High Cohesion and Low Coupling principles
Reduce dependencies between code/documentation/terminology parts and keep related code/documentation/terminology together.

## Be consistent with existing code/documentation/terminology

# Code instructions

## Use fail fast approach by default
Don't catch exceptions unless it's top level code or you re-raise them with more context.

## Use early returns to reduce nesting

## Don't add comments
- Prefere self-explanatory code and don't add comments unless it's absolutely necessary to describe why the code does something in a specific way;
- Never add comments to describe what the code does, it should be clear from the code itself.

## Use encapsulation intensively
- Make evything private by default and add public methods/properties only when it's explicitly needed.
- Add "__" prefix for private functions.

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