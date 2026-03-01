import json
import toons


def format_object(obj, format: str) -> str:
    if format == 'json':
        return json.dumps(obj, ensure_ascii=False)
    
    if format == 'json_with_indent':
        return json.dumps(obj, ensure_ascii=False, indent=2)

    if format == 'toon':
        return toons.dumps(obj)

    raise ValueError(f"Unsupported format: {format}")
