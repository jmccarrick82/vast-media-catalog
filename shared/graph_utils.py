"""Shared graph traversal utilities for the relationships table.

All functions accept either a PyArrow Table (from vast.read_table) or
pre-built adjacency dicts. Build adjacency once, reuse across analyses.
"""

import pyarrow as pa


def build_adjacency(rel_table: pa.Table) -> tuple[dict, dict]:
    """Build bidirectional adjacency maps from a relationships table.

    Returns:
        (parent_to_children, child_to_parents) where:
        - parent_to_children: {parent_id: [(child_id, rel_type), ...]}
        - child_to_parents:   {child_id: [(parent_id, rel_type), ...]}
    """
    parent_to_children = {}
    child_to_parents = {}

    if rel_table is None or rel_table.num_rows == 0:
        return parent_to_children, child_to_parents

    parents = rel_table.column("parent_asset_id").to_pylist()
    children = rel_table.column("child_asset_id").to_pylist()

    rel_types = None
    if "relationship_type" in rel_table.column_names:
        rel_types = rel_table.column("relationship_type").to_pylist()

    for i in range(len(parents)):
        p, c = parents[i], children[i]
        rt = rel_types[i] if rel_types else "unknown"

        parent_to_children.setdefault(p, []).append((c, rt))
        child_to_parents.setdefault(c, []).append((p, rt))

    return parent_to_children, child_to_parents


def find_parents(child_to_parents: dict, asset_id: str) -> list[str]:
    """Return parent asset IDs for a given asset."""
    return [p for p, _ in child_to_parents.get(asset_id, [])]


def find_parents_with_types(child_to_parents: dict, asset_id: str) -> list[tuple[str, str]]:
    """Return (parent_id, relationship_type) tuples for a given asset."""
    return child_to_parents.get(asset_id, [])


def find_children(parent_to_children: dict, asset_id: str) -> list[str]:
    """Return child asset IDs for a given asset."""
    return [c for c, _ in parent_to_children.get(asset_id, [])]


def find_children_with_types(parent_to_children: dict, asset_id: str) -> list[tuple[str, str]]:
    """Return (child_id, relationship_type) tuples for a given asset."""
    return parent_to_children.get(asset_id, [])


def find_all_ancestors(child_to_parents: dict, asset_id: str, max_depth: int = 50) -> list[tuple[str, int, str]]:
    """BFS upward to find all ancestors.

    Returns list of (ancestor_id, depth, relationship_type) ordered by depth.
    """
    result = []
    visited = {asset_id}
    queue = [(asset_id, 0)]

    while queue:
        current, depth = queue.pop(0)
        if depth >= max_depth:
            continue
        for parent, rel_type in child_to_parents.get(current, []):
            if parent not in visited:
                visited.add(parent)
                result.append((parent, depth + 1, rel_type))
                queue.append((parent, depth + 1))

    return result


def count_descendants(parent_to_children: dict, asset_id: str) -> int:
    """BFS downward to count all descendants (children, grandchildren, etc.)."""
    visited = {asset_id}
    queue = [asset_id]
    count = 0

    while queue:
        current = queue.pop(0)
        for child, _ in parent_to_children.get(current, []):
            if child not in visited:
                visited.add(child)
                count += 1
                queue.append(child)

    return count


def trace_chain_downward(parent_to_children: dict, asset_id: str) -> list[dict]:
    """BFS downward returning all descendants with relationship info.

    Returns list of {asset_id, parent_id, relationship_type, depth}.
    """
    result = []
    visited = {asset_id}
    queue = [(asset_id, 0)]

    while queue:
        current, depth = queue.pop(0)
        for child, rel_type in parent_to_children.get(current, []):
            if child not in visited:
                visited.add(child)
                result.append({
                    "asset_id": child,
                    "parent_id": current,
                    "relationship_type": rel_type,
                    "depth": depth + 1,
                })
                queue.append((child, depth + 1))

    return result


def has_relationship(child_to_parents: dict, parent_to_children: dict, asset_id: str) -> bool:
    """Check if an asset has any relationships (as parent or child)."""
    return bool(child_to_parents.get(asset_id)) or bool(parent_to_children.get(asset_id))


def trace_root(child_to_parents: dict, asset_id: str) -> list[str]:
    """Walk upward following first parent at each level. Returns path from root to asset."""
    chain = [asset_id]
    visited = {asset_id}
    current = asset_id

    while True:
        parents = child_to_parents.get(current, [])
        if not parents:
            break
        parent = parents[0][0]
        if parent in visited:
            break
        visited.add(parent)
        chain.append(parent)
        current = parent

    chain.reverse()
    return chain
