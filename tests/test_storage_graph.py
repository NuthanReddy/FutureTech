import importlib.util
import json
from pathlib import Path
import sys

import pytest


@pytest.fixture(scope="module")
def storage_graph_module():
    module_path = (
        Path(__file__).resolve().parents[1]
        / "Misc"
        / "Storage Visualizer"
        / "storage_graph.py"
    )
    spec = importlib.util.spec_from_file_location("storage_graph", module_path)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def write_file(path: Path, size: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)


def child_map(node):
    return {child.name: child for child in node.children}


def test_import_exposes_helpers_without_launching_ui(storage_graph_module) -> None:
    assert hasattr(storage_graph_module, "scan_directory")
    assert hasattr(storage_graph_module, "apply_display_grouping")
    assert hasattr(storage_graph_module, "build_chart_segments")


def test_scan_directory_builds_sizes_and_parent_links(storage_graph_module, tmp_path: Path) -> None:
    write_file(tmp_path / "a.txt", 10)
    write_file(tmp_path / "sub" / "b.bin", 30)
    write_file(tmp_path / "sub" / "c.log", 5)

    root = storage_graph_module.scan_directory(tmp_path)
    children = child_map(root)

    assert root.size == 45
    assert children["a.txt"].size == 10
    assert children["sub"].size == 35
    assert children["sub"].parent is root
    assert children["sub"].children == []
    assert children["sub"].children_loaded is False


def test_scan_directory_keeps_empty_directories(storage_graph_module, tmp_path: Path) -> None:
    (tmp_path / "empty").mkdir()
    write_file(tmp_path / "file.txt", 7)

    root = storage_graph_module.scan_directory(tmp_path)
    children = child_map(root)

    assert root.size == 7
    assert children["empty"].is_dir is True
    assert children["empty"].size == 0


def test_grouping_creates_others_when_combined_small_share_reaches_threshold(
    storage_graph_module, tmp_path: Path
) -> None:
    write_file(tmp_path / "big.dat", 90)
    write_file(tmp_path / "small1.txt", 3)
    write_file(tmp_path / "small2.txt", 2)

    root = storage_graph_module.scan_directory(tmp_path)
    grouped = storage_graph_module.apply_display_grouping(root, min_percent=5.0, enabled=True)
    children = child_map(grouped)

    assert "Others" in children
    assert children["Others"].is_group is True
    assert children["Others"].size == 5
    assert "big.dat" in children
    assert "small1.txt" not in children
    assert "small2.txt" not in children
    assert len(root.children) == 3


def test_grouping_does_not_create_others_below_threshold(storage_graph_module, tmp_path: Path) -> None:
    write_file(tmp_path / "big.dat", 97)
    write_file(tmp_path / "small1.txt", 2)
    write_file(tmp_path / "small2.txt", 2)

    root = storage_graph_module.scan_directory(tmp_path)
    grouped = storage_graph_module.apply_display_grouping(root, min_percent=5.0, enabled=True)
    children = child_map(grouped)

    assert "Others" not in children
    assert "small1.txt" in children
    assert "small2.txt" in children


def test_load_immediate_children_populates_on_demand(storage_graph_module, tmp_path: Path) -> None:
    write_file(tmp_path / "folder_a" / "nested.txt", 20)
    write_file(tmp_path / "folder_b" / "leaf.txt", 10)

    root = storage_graph_module.scan_directory(tmp_path)
    folder_a = child_map(root)["folder_a"]

    assert folder_a.children == []
    assert folder_a.children_loaded is False

    storage_graph_module.load_immediate_children(folder_a)
    loaded_children = child_map(folder_a)

    assert folder_a.children_loaded is True
    assert "nested.txt" in loaded_children
    assert loaded_children["nested.txt"].size == 20


def test_build_chart_segments_only_shows_current_level(storage_graph_module, tmp_path: Path) -> None:
    write_file(tmp_path / "folder_a" / "nested.txt", 20)
    write_file(tmp_path / "folder_b" / "leaf.txt", 10)
    write_file(tmp_path / "top.txt", 10)

    root = storage_graph_module.scan_directory(tmp_path)
    display_root = storage_graph_module.apply_display_grouping(root, enabled=False)
    segments = storage_graph_module.build_chart_segments(
        display_root,
        center_radius=40,
        ring_width=30,
    )

    segment_names = {segment.node.name for segment in segments}
    assert {"folder_a", "folder_b", "top.txt"}.issubset(segment_names)
    assert "nested.txt" not in segment_names
    assert "leaf.txt" not in segment_names
    assert all(segment.extent > 0 for segment in segments)


def test_scan_writes_local_cache_with_last_scan_timestamp(storage_graph_module, tmp_path: Path) -> None:
    write_file(tmp_path / "sub" / "item.bin", 12)
    cache_file = tmp_path / ".storage_graph_cache.json"

    root = storage_graph_module.scan_directory(tmp_path, cache_file=cache_file)

    assert root.size == 12
    assert cache_file.exists()

    payload = json.loads(cache_file.read_text(encoding="utf-8"))
    assert payload["root_path"] == str(tmp_path.resolve())
    assert payload["last_scan_timestamp"]
    assert str(tmp_path.resolve()) in payload["nodes"]


def test_incremental_scan_skips_unchanged_folder_and_rescans_changed_folder(
    storage_graph_module, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sub_dir = tmp_path / "sub"
    write_file(sub_dir / "a.txt", 8)
    cache_file = tmp_path / ".storage_graph_cache.json"

    first = storage_graph_module.scan_directory(tmp_path, cache_file=cache_file)
    assert first.size == 8

    original_scandir = storage_graph_module.os.scandir
    tracked_path = sub_dir.resolve()
    calls = {"sub": 0}

    def counting_scandir(path):
        if Path(path).resolve() == tracked_path:
            calls["sub"] += 1
        return original_scandir(path)

    monkeypatch.setattr(storage_graph_module.os, "scandir", counting_scandir)

    second = storage_graph_module.scan_directory(tmp_path, cache_file=cache_file)
    assert second.size == 8
    assert calls["sub"] == 0

    write_file(sub_dir / "b.txt", 5)
    third = storage_graph_module.scan_directory(tmp_path, cache_file=cache_file)
    assert third.size == 13
    assert calls["sub"] >= 1


