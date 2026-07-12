import re
from typing import Any

_FRONTMATTER_DELIMITER = '---'
_REQUIRED_SKILL_FIELDS = ('name', 'description')


def parse_skill_markdown(content: str) -> dict[str, Any]:
    frontmatter, body = split_frontmatter(content)
    metadata = parse_simple_yaml(frontmatter) if frontmatter is not None else {}
    warnings = validate_skill_metadata(metadata)
    return {'frontmatter': metadata, 'body': body, 'warnings': warnings}


def split_frontmatter(content: str) -> tuple[str | None, str]:
    normalized = content.replace(chr(13) + chr(10), chr(10))
    marker = _FRONTMATTER_DELIMITER + chr(10)
    if not normalized.startswith(marker):
        return None, content
    end_marker = chr(10) + _FRONTMATTER_DELIMITER + chr(10)
    end_index = normalized.find(end_marker, len(marker))
    if end_index == -1:
        return None, content
    frontmatter = normalized[len(marker):end_index]
    body = normalized[end_index + len(end_marker):]
    return frontmatter, body


def parse_simple_yaml(frontmatter: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    current_map_key: str | None = None
    for raw_line in frontmatter.splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith('#'):
            continue
        if raw_line.startswith((' ', chr(9))):
            if current_map_key is None:
                continue
            nested_match = re.match(r'^\s+([^:]+):\s*(.*)$', raw_line)
            if nested_match:
                nested_key = nested_match.group(1).strip()
                nested_value = _clean_scalar(nested_match.group(2).strip())
                nested_map = result.setdefault(current_map_key, {})
                if isinstance(nested_map, dict):
                    nested_map[nested_key] = nested_value
            continue
        key, separator, value = raw_line.partition(':')
        if not separator:
            continue
        current_map_key = None
        key = key.strip()
        value = value.strip()
        if value == '':
            result[key] = {}
            current_map_key = key
        else:
            result[key] = _clean_scalar(value)
    return result


def validate_skill_metadata(metadata: dict[str, Any]) -> list[str]:
    warnings = []
    for field in _REQUIRED_SKILL_FIELDS:
        if not str(metadata.get(field, '')).strip():
            warnings.append(f'Missing required skill frontmatter field: {field}')
    name = str(metadata.get('name', '')).strip()
    if name and not re.fullmatch(r'[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?', name):
        warnings.append('Skill frontmatter field name should be lowercase letters, numbers, and hyphens only')
    if '--' in name:
        warnings.append('Skill frontmatter field name should not contain consecutive hyphens')
    description = str(metadata.get('description', '')).strip()
    if len(description) > 1024:
        warnings.append('Skill frontmatter field description should be at most 1024 characters')
    return warnings


def _clean_scalar(value: str) -> str:
    if len(value) >= 2 and value.startswith(chr(34)) and value.endswith(chr(34)):
        return value[1:-1]
    if len(value) >= 2 and value.startswith(chr(39)) and value.endswith(chr(39)):
        return value[1:-1]
    return value
