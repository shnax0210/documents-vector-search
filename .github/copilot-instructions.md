# Code instructions

1. Don't add comments. Prefere self-explanatory code.
2. Add "__" prefix for private variables and methods.
3. Use fail fast approach by default. Don't catch exceptions unless it's top level code or you re-raise them with more context.
4. Keep it simple. Start from minimal working code and iterate when explicitly asked.
5. Use `uv` for dependency management (`uv add <package>`) and code execution (`uv run <script>`). Don't use `pip` or `python` directly.
6. Be consistent with existing code style.
