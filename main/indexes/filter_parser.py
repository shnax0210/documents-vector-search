import re
from dataclasses import dataclass, field
from typing import List, Optional, Union


@dataclass
class FilterCondition:
    field: str
    operator: str
    value: str


@dataclass
class FilterGroup:
    logical_operator: str
    children: List[Union[FilterCondition, "FilterGroup"]] = field(default_factory=list)


FilterNode = Union[FilterCondition, FilterGroup]

__TOKEN_PATTERN = re.compile(
    r'\s*(?:'
    r'(\()|'
    r'(\))|'
    r'(and|or)\b|'
    r'(\w+)\s*(!=|>=|<=|=|>|<)\s*"([^"]*)"'
    r')\s*',
    re.IGNORECASE,
)


def parse_filter(filter_str: str) -> Optional[FilterNode]:
    if not filter_str:
        return None

    tokens = __tokenize(filter_str.strip())
    node, pos = __parse_expression(tokens, 0)
    if pos != len(tokens):
        raise ValueError(f"Unexpected token at position {pos}: {tokens[pos]}")
    return node


def __tokenize(text):
    tokens = []
    pos = 0
    while pos < len(text):
        m = __TOKEN_PATTERN.match(text, pos)
        if not m:
            raise ValueError(f"Unexpected character at position {pos}: '{text[pos:]}'")

        if m.group(1):
            tokens.append(("LPAREN",))
        elif m.group(2):
            tokens.append(("RPAREN",))
        elif m.group(3):
            tokens.append(("LOGICAL", m.group(3).lower()))
        elif m.group(4):
            tokens.append(("CONDITION", FilterCondition(field=m.group(4), operator=m.group(5), value=m.group(6))))

        pos = m.end()
    return tokens


def __parse_expression(tokens, pos):
    left, pos = __parse_primary(tokens, pos)

    while pos < len(tokens) and tokens[pos][0] == "LOGICAL":
        op = tokens[pos][1]
        pos += 1
        right, pos = __parse_primary(tokens, pos)

        if isinstance(left, FilterGroup) and left.logical_operator == op:
            left.children.append(right)
        else:
            left = FilterGroup(logical_operator=op, children=[left, right])

    return left, pos


def __parse_primary(tokens, pos):
    if pos >= len(tokens):
        raise ValueError("Unexpected end of filter expression")

    if tokens[pos][0] == "LPAREN":
        pos += 1
        node, pos = __parse_expression(tokens, pos)
        if pos >= len(tokens) or tokens[pos][0] != "RPAREN":
            raise ValueError("Missing closing parenthesis")
        pos += 1
        return node, pos

    if tokens[pos][0] == "CONDITION":
        return tokens[pos][1], pos + 1

    raise ValueError(f"Unexpected token: {tokens[pos]}")

