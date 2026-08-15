from typing import List, Tuple

from main.indexes.filter_parser import FilterNode, FilterCondition


def convert_filter_to_sql(node: FilterNode) -> Tuple[str, List[str]]:
    if isinstance(node, FilterCondition):
        return f"json_extract(data, '$.{node.field}') {node.operator} ?", [node.value]

    child_sqls = []
    params = []
    for child in node.children:
        child_sql, child_params = convert_filter_to_sql(child)
        child_sqls.append(child_sql)
        params.extend(child_params)

    joiner = " AND " if node.logical_operator == "and" else " OR "
    return f"({joiner.join(child_sqls)})", params
