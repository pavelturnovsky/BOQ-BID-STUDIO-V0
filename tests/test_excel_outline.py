from core.excel_outline import build_outline_nodes


def test_build_outline_nodes_nested_structure():
    level_map = {
        2: {"level": 1, "hidden": False},
        3: {"level": 2, "hidden": True},
        4: {"level": 2, "hidden": False},
        5: {"level": 1, "hidden": False},
    }

    nodes = build_outline_nodes(level_map, axis="row", sheet="List1")
    assert len(nodes) == 1
    root = nodes[0]
    assert root.level == 1
    assert root.start == 2 and root.end == 5
    assert root.collapsed is True
    assert len(root.children) == 1
    child = root.children[0]
    assert child.level == 2
    assert child.start == 3 and child.end == 4
    assert child.collapsed is True
