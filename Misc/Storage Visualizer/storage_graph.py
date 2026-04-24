"""Interactive storage visualizer with drill-down donut charts.

This script scans a directory tree and renders storage consumption as a
multi-level donut chart. Clicking a folder arc drills into that folder,
allowing the next level of the hierarchy to become the new focus.

The UI is intentionally self-contained and uses only the Python standard
library so it can run in this repository without adding new dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import subprocess
from typing import Callable, Iterable, Optional


DEFAULT_GROUP_THRESHOLD = 5.0
CACHE_FILE_NAME = ".storage_graph_cache.json"
CACHE_VERSION = 1

# Rich 50-color palette with good contrast on dark backgrounds.
CHART_PALETTE = (
	"#38bdf8", "#60a5fa", "#818cf8", "#a78bfa", "#c084fc",
	"#e879f9", "#f472b6", "#fb7185", "#f59e0b", "#f97316",
	"#34d399", "#22d3ee", "#2dd4bf", "#4ade80", "#a3e635",
	"#facc15", "#fb923c", "#f87171", "#c084fc", "#67e8f9",
	"#86efac", "#fbbf24", "#e879f9", "#7dd3fc", "#d946ef",
	"#a78bfa", "#fca5a1", "#6ee7b7", "#93c5fd", "#fcd34d",
	"#f0abfc", "#5eead4", "#bef264", "#fdba74", "#f9a8d4",
	"#7c3aed", "#0891b2", "#059669", "#d97706", "#dc2626",
	"#4f46e5", "#0d9488", "#65a30d", "#ea580c", "#be185d",
	"#7e22ce", "#0e7490", "#15803d", "#c2410c", "#9f1239",
)


def _hex_to_rgb(color: str) -> tuple[int, int, int]:
	"""Convert a #RRGGBB color string to RGB channels."""

	color = color.lstrip("#")
	return int(color[0:2], 16), int(color[2:4], 16), int(color[4:6], 16)


def _rgb_to_hex(red: int, green: int, blue: int) -> str:
	"""Convert integer RGB channels to a #RRGGBB color string."""

	return f"#{max(0, min(255, red)):02x}{max(0, min(255, green)):02x}{max(0, min(255, blue)):02x}"


def _mix_color(color: str, target: str, ratio: float) -> str:
	"""Blend *color* towards *target* using *ratio* in [0, 1]."""

	ratio = max(0.0, min(1.0, ratio))
	c_red, c_green, c_blue = _hex_to_rgb(color)
	t_red, t_green, t_blue = _hex_to_rgb(target)
	return _rgb_to_hex(
		int(c_red + (t_red - c_red) * ratio),
		int(c_green + (t_green - c_green) * ratio),
		int(c_blue + (t_blue - c_blue) * ratio),
	)


def _stable_color(label: str, depth: int) -> str:
	"""Create a stable, readable color derived from *label* and *depth*."""

	seed = sum((index + 1) * ord(char) for index, char in enumerate(label))
	base = CHART_PALETTE[seed % len(CHART_PALETTE)]
	# Inner rings are slightly brighter; outer rings are slightly muted.
	if depth <= 1:
		return _mix_color(base, "#ffffff", 0.12)
	return _mix_color(base, "#0b1120", min(0.34, 0.08 * (depth - 1)))


def _hover_color(color: str) -> str:
	"""Return a brighter color variant used for hover emphasis."""

	return _mix_color(color, "#ffffff", 0.24)


def _label_color_for_segment(fill_color: str) -> str:
	"""Choose a legible text color for a segment fill color."""

	red, green, blue = _hex_to_rgb(fill_color)
	# Perceived luminance for contrast-aware text color.
	luminance = (0.2126 * red + 0.7152 * green + 0.0722 * blue) / 255
	return "#0b1120" if luminance > 0.62 else "#f8fafc"


def _cache_path_for_scan(root: Path) -> Path:
	"""Return the default cache path for a scan rooted at *root*."""
	if root.is_dir():
		return root / CACHE_FILE_NAME
	return root.parent / CACHE_FILE_NAME


def _load_scan_cache(cache_path: Path) -> dict[str, object]:
	"""Load scan cache content from disk, returning an empty cache on failure."""
	if not cache_path.exists():
		return {"version": CACHE_VERSION, "nodes": {}}
	try:
		payload = json.loads(cache_path.read_text(encoding="utf-8"))
	except (OSError, json.JSONDecodeError):
		return {"version": CACHE_VERSION, "nodes": {}}
	if not isinstance(payload, dict):
		return {"version": CACHE_VERSION, "nodes": {}}
	if not isinstance(payload.get("nodes"), dict):
		payload["nodes"] = {}
	payload["version"] = CACHE_VERSION
	return payload


def _write_scan_cache(cache_path: Path, cache_data: dict[str, object]) -> None:
	"""Persist scan cache content to disk using an atomic replace."""
	cache_path.parent.mkdir(parents=True, exist_ok=True)
	temp_path = cache_path.with_suffix(cache_path.suffix + ".tmp")
	temp_path.write_text(json.dumps(cache_data, indent=2, sort_keys=True), encoding="utf-8")
	temp_path.replace(cache_path)


@dataclass(slots=True)
class StorageNode:
	"""Represents a file system entry in the scanned tree."""
	name: str
	path: Path
	size: int
	is_dir: bool
	children: list["StorageNode"] = field(default_factory=list)
	parent: Optional["StorageNode"] = field(default=None, repr=False, compare=False)
	inaccessible_children: int = 0
	children_loaded: bool = False


@dataclass(slots=True)
class DisplayNode:
	"""Represents a display-ready node used by the chart renderer."""
	name: str
	size: int
	is_dir: bool
	children: list["DisplayNode"] = field(default_factory=list)
	source_node: Optional[StorageNode] = None
	is_group: bool = False


@dataclass(slots=True)
class ChartSegment:
	"""Describes one clickable arc in the donut chart."""
	node: DisplayNode
	start_angle: float
	extent: float
	inner_radius: float
	outer_radius: float
	depth: int
	percent_of_parent: float
	item_id: Optional[int] = None

	def contains_point(self, x: float, y: float, center_x: float, center_y: float) -> bool:
		"""Return True when a canvas point falls inside the segment."""
		dx = x - center_x
		dy = center_y - y
		radius = math.hypot(dx, dy)
		if radius < self.inner_radius or radius > self.outer_radius:
			return False
		angle = math.degrees(math.atan2(dy, dx))
		if angle < 0:
			angle += 360
		delta = (angle - self.start_angle) % 360
		return 0 <= delta <= self.extent or math.isclose(delta, self.extent)


def format_size(size_bytes: int) -> str:
	"""Convert bytes to a human-readable string."""

	value = float(size_bytes)
	for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
		if value < 1024 or unit == "PB":
			return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
		value /= 1024
	return f"{size_bytes} B"


def scan_directory(
	root_path: str | Path,
	*,
	follow_symlinks: bool = False,
	on_error: Optional[Callable[[Path, Exception], None]] = None,
	cache_file: Optional[str | Path] = None,
) -> StorageNode:
	"""Scan *root_path* and return a lazily loaded storage tree.

	Args:
		root_path: Directory or file path to scan.
		follow_symlinks: Whether symlink targets may be traversed.
		on_error: Optional callback used for permission or stat failures.
	"""

	root = Path(root_path).expanduser().resolve()
	if not root.exists():
		raise FileNotFoundError(f"Path does not exist: {root}")

	cache_path = Path(cache_file).expanduser().resolve() if cache_file else _cache_path_for_scan(root)
	cache_payload = _load_scan_cache(cache_path)
	nodes_cache = cache_payload.get("nodes", {})
	if not isinstance(nodes_cache, dict):
		nodes_cache = {}

	def cache_key(path: Path) -> str:
		return str(path)

	def read_cached_dir_size(path: Path, mtime_ns: int = 0) -> Optional[int]:
		cached = nodes_cache.get(cache_key(path))
		if not isinstance(cached, dict):
			return None
		if not cached.get("is_dir"):
			return None
		size = cached.get("size")
		return int(size) if isinstance(size, int) else None

	def update_cache_entry(path: Path, *, is_dir: bool, size: int, mtime_ns: Optional[int]) -> None:
		nodes_cache[cache_key(path)] = {
			"is_dir": is_dir,
			"size": size,
			"mtime_ns": mtime_ns,
		}

	def compute_directory_size(path: Path) -> int:
		try:
			dir_stat = path.stat(follow_symlinks=follow_symlinks)
		except (OSError, PermissionError) as exc:
			if on_error:
				on_error(path, exc)
			return 0

		cached_size = read_cached_dir_size(path, dir_stat.st_mtime_ns)
		if cached_size is not None:
			return cached_size

		total_size = 0
		try:
			with os.scandir(path) as iterator:
				entries = sorted(iterator, key=lambda entry: entry.name.lower())
		except (OSError, PermissionError) as exc:
			if on_error:
				on_error(path, exc)
			return 0

		for entry in entries:
			child_path = Path(entry.path)
			try:
				if child_path == cache_path:
					continue

				if entry.is_symlink() and not follow_symlinks:
					continue

				if entry.is_dir(follow_symlinks=follow_symlinks):
					total_size += compute_directory_size(child_path)
				else:
					total_size += entry.stat(follow_symlinks=follow_symlinks).st_size
			except (FileNotFoundError, OSError, PermissionError) as exc:
				if on_error:
					on_error(child_path, exc)

		update_cache_entry(path, is_dir=True, size=total_size, mtime_ns=dir_stat.st_mtime_ns)
		return total_size

	def build_node(path: Path, parent: Optional[StorageNode], *, load_children: bool) -> StorageNode:
		name = path.name or path.anchor or str(path)
		is_dir = path.is_dir()

		if not is_dir:
			file_stat = path.stat(follow_symlinks=follow_symlinks)
			size = file_stat.st_size
			update_cache_entry(path, is_dir=False, size=size, mtime_ns=file_stat.st_mtime_ns)
			return StorageNode(name=name, path=path, size=size, is_dir=False, parent=parent, children_loaded=True)

		node = StorageNode(name=name, path=path, size=0, is_dir=True, parent=parent)
		try:
			dir_stat = path.stat(follow_symlinks=follow_symlinks)
		except (OSError, PermissionError) as exc:
			node.inaccessible_children += 1
			if on_error:
				on_error(path, exc)
			return node

		if not load_children:
			cached_size = read_cached_dir_size(path, dir_stat.st_mtime_ns)
			node.size = cached_size if cached_size is not None else compute_directory_size(path)
			update_cache_entry(path, is_dir=True, size=node.size, mtime_ns=dir_stat.st_mtime_ns)
			return node

		try:
			with os.scandir(path) as iterator:
				entries = sorted(iterator, key=lambda entry: entry.name.lower())
		except (OSError, PermissionError) as exc:
			node.inaccessible_children += 1
			if on_error:
				on_error(path, exc)
			return node

		for entry in entries:
			child_path = Path(entry.path)
			try:
				if child_path == cache_path:
					continue

				if entry.is_symlink() and not follow_symlinks:
					continue

				if entry.is_dir(follow_symlinks=follow_symlinks):
					child_node = build_node(child_path, node, load_children=False)
				else:
					size = entry.stat(follow_symlinks=follow_symlinks).st_size
					child_node = StorageNode(
						name=entry.name,
						path=child_path,
						size=size,
						is_dir=False,
						parent=node,
						children_loaded=True,
					)

				node.children.append(child_node)
				node.size += child_node.size
			except (FileNotFoundError, OSError, PermissionError) as exc:
				node.inaccessible_children += 1
				if on_error:
					on_error(child_path, exc)

		node.children_loaded = True
		update_cache_entry(path, is_dir=True, size=node.size, mtime_ns=dir_stat.st_mtime_ns)
		return node

	root_node = build_node(root, None, load_children=True)
	cache_payload["version"] = CACHE_VERSION
	cache_payload["root_path"] = str(root)
	cache_payload["follow_symlinks"] = follow_symlinks
	cache_payload["last_scan_timestamp"] = datetime.now(timezone.utc).isoformat()
	cache_payload["nodes"] = nodes_cache
	try:
		_write_scan_cache(cache_path, cache_payload)
	except OSError as exc:
		if on_error:
			on_error(cache_path, exc)

	return root_node


def load_immediate_children(
	node: StorageNode,
	*,
	follow_symlinks: bool = False,
	on_error: Optional[Callable[[Path, Exception], None]] = None,
) -> None:
	"""Populate one folder level for *node* if it has not been loaded yet."""

	if not node.is_dir or node.children_loaded:
		return

	children: list[StorageNode] = []
	inaccessible_children = node.inaccessible_children

	def compute_directory_size(path: Path) -> int:
		total_size = 0
		try:
			with os.scandir(path) as iterator:
				entries = sorted(iterator, key=lambda entry: entry.name.lower())
		except (OSError, PermissionError) as exc:
			if on_error:
				on_error(path, exc)
			return 0

		for entry in entries:
			child_path = Path(entry.path)
			try:
				if entry.is_symlink() and not follow_symlinks:
					continue
				if entry.is_dir(follow_symlinks=follow_symlinks):
					total_size += compute_directory_size(child_path)
				else:
					total_size += entry.stat(follow_symlinks=follow_symlinks).st_size
			except (FileNotFoundError, OSError, PermissionError) as exc:
				if on_error:
					on_error(child_path, exc)

		return total_size

	try:
		with os.scandir(node.path) as iterator:
			entries = sorted(iterator, key=lambda entry: entry.name.lower())
	except (OSError, PermissionError) as exc:
		inaccessible_children += 1
		if on_error:
			on_error(node.path, exc)
		node.children = []
		node.size = 0
		node.inaccessible_children = inaccessible_children
		node.children_loaded = True
		return

	total_size = 0
	for entry in entries:
		child_path = Path(entry.path)
		try:
			if entry.is_symlink() and not follow_symlinks:
				continue

			if entry.is_dir(follow_symlinks=follow_symlinks):
				child_size = compute_directory_size(child_path)
				child_node = StorageNode(
					name=entry.name,
					path=child_path,
					size=child_size,
					is_dir=True,
					parent=node,
				)
			else:
				child_size = entry.stat(follow_symlinks=follow_symlinks).st_size
				child_node = StorageNode(
					name=entry.name,
					path=child_path,
					size=child_size,
					is_dir=False,
					parent=node,
					children_loaded=True,
				)

			children.append(child_node)
			total_size += child_size
		except (FileNotFoundError, OSError, PermissionError) as exc:
			inaccessible_children += 1
			if on_error:
				on_error(child_path, exc)

	node.children = children
	node.size = total_size
	node.inaccessible_children = inaccessible_children
	node.children_loaded = True


def apply_display_grouping(
	node: StorageNode,
	*,
	min_percent: float = DEFAULT_GROUP_THRESHOLD,
	enabled: bool = False,
) -> DisplayNode:
	"""Return a display tree with optional `Others` grouping.

	Small direct children of a folder are grouped only when grouping is enabled,
	each item is below *min_percent* of the folder, and the combined percentage of
	those small items is at least *min_percent*.
	"""

	grouped_children = [
		apply_display_grouping(child, min_percent=min_percent, enabled=enabled)
		for child in sorted(node.children, key=lambda child: (-child.size, child.name.lower()))
	]

	if not enabled or node.size <= 0 or len(grouped_children) <= 1:
		return DisplayNode(
			name=node.name,
			size=node.size,
			is_dir=node.is_dir,
			children=grouped_children,
			source_node=node,
		)

	threshold = max(min_percent, 0.0)

	# Sort children smallest-first so we greedily pick the tiniest items
	# until their combined share reaches the threshold.
	candidates = sorted(grouped_children, key=lambda c: c.size)
	small_children: list[DisplayNode] = []
	small_total = 0

	for child in candidates:
		percent = child.size / node.size * 100 if node.size else 0
		if percent >= threshold:
			# This item is big enough to stand on its own; stop collecting.
			break
		tentative = small_total + child.size
		tentative_pct = tentative / node.size * 100 if node.size else 0
		small_children.append(child)
		small_total = tentative
		if tentative_pct >= threshold:
			break

	combined_percent = small_total / node.size * 100 if node.size else 0
	small_set = set(id(c) for c in small_children)
	visible_children = [c for c in grouped_children if id(c) not in small_set]

	if len(small_children) > 1 and combined_percent >= threshold:
		visible_children.append(
			DisplayNode(
				name="Others",
				size=small_total,
				is_dir=True,
				children=small_children,
				source_node=None,
				is_group=True,
			)
		)
	else:
		# Not enough small items to form a meaningful group; show them individually.
		visible_children.extend(small_children)

	visible_children.sort(key=lambda child: (-child.size, child.name.lower()))
	return DisplayNode(
		name=node.name,
		size=node.size,
		is_dir=node.is_dir,
		children=visible_children,
		source_node=node,
	)


def count_descendants(node: StorageNode) -> tuple[int, int]:
	"""Return the number of files and directories under *node*."""

	if not node.is_dir:
		return 1, 0

	file_count = 0
	dir_count = 0
	for child in node.children:
		if child.is_dir:
			dir_count += 1
			nested_files, nested_dirs = count_descendants(child)
			file_count += nested_files
			dir_count += nested_dirs
		else:
			file_count += 1
	return file_count, dir_count


def breadcrumbs(node: StorageNode) -> list[StorageNode]:
	"""Return the breadcrumb chain from the scan root to *node*."""

	chain: list[StorageNode] = []
	current: Optional[StorageNode] = node
	while current is not None:
		chain.append(current)
		current = current.parent
	return list(reversed(chain))


def build_chart_segments(
	root: DisplayNode,
	*,
	max_levels: int = 1,
	center_radius: float,
	ring_width: float,
	ring_gap: float = 10.0,
) -> list[ChartSegment]:
	"""Build drawable donut segments for the visible part of the tree."""

	segments: list[ChartSegment] = []

	def walk(node: DisplayNode, start_angle: float, sweep: float, depth: int) -> None:
		if depth >= max_levels or node.size <= 0:
			return

		inner_radius = center_radius + depth * ring_width
		outer_radius = inner_radius + ring_width - ring_gap
		current_angle = start_angle

		for child in node.children:
			if child.size <= 0:
				continue

			extent = sweep * (child.size / node.size)
			if extent <= 0:
				continue

			percent = child.size / node.size * 100
			segments.append(
				ChartSegment(
					node=child,
					start_angle=current_angle,
					extent=extent,
					inner_radius=inner_radius,
					outer_radius=outer_radius,
					depth=depth + 1,
					percent_of_parent=percent,
				)
			)

			# Recurse into children for multi-level sunburst rings.
			if child.children and not child.is_group:
				walk(child, current_angle, extent, depth + 1)

			current_angle += extent

	walk(root, start_angle=90.0, sweep=360.0, depth=0)
	return segments



def launch_app() -> None:
	"""Launch the Tkinter desktop application."""

	# Enable high-DPI awareness on Windows so the canvas renders at native resolution.
	if os.name == "nt":
		try:
			import ctypes
			ctypes.windll.shcore.SetProcessDpiAwareness(2)  # Per-monitor DPI aware
		except Exception:
			try:
				ctypes.windll.user32.SetProcessDPIAware()
			except Exception:
				pass

	import tkinter as tk
	from tkinter import filedialog, messagebox, ttk

	class StorageVisualizerApp(tk.Tk):
		"""Desktop UI for exploring storage distribution."""

		def __init__(self) -> None:
			super().__init__()
			self.title("Storage Consumption Visualizer")
			self.geometry("1280x820")
			self.minsize(1024, 680)
			self.configure(bg="#0f172a")

			# Set window icon
			icon_path = Path(__file__).parent / "icon.ico"
			if icon_path.exists():
				try:
					self.iconbitmap(str(icon_path))
				except Exception:
					pass

			# Dark title bar on Windows 10/11
			if os.name == "nt":
				try:
					import ctypes
					self.update()  # Ensure window is mapped before querying hwnd
					hwnd = ctypes.windll.user32.GetParent(self.winfo_id())
					DWMWA_USE_IMMERSIVE_DARK_MODE = 20
					value = ctypes.c_int(1)
					ctypes.windll.dwmapi.DwmSetWindowAttribute(
						hwnd, DWMWA_USE_IMMERSIVE_DARK_MODE,
						ctypes.byref(value), ctypes.sizeof(value),
					)
				except Exception:
					pass

			self.root_node: Optional[StorageNode] = None
			self.current_node: Optional[StorageNode] = None
			self.history: list[StorageNode] = []
			self.display_root: Optional[DisplayNode] = None
			self.segments: list[ChartSegment] = []
			self.hovered_segment: Optional[ChartSegment] = None
			self.segment_styles: dict[int, dict[str, object]] = {}
			self.visible_tree_nodes: list[DisplayNode] = []
			self.selected_display_node: Optional[DisplayNode] = None

			self.path_var = tk.StringVar(value="")
			self.status_var = tk.StringVar(value="Choose a folder and click Scan.")
			self.breadcrumb_var = tk.StringVar(value="")
			self.selection_var = tk.StringVar(value="Hover over a segment to inspect it.")
			self.group_var = tk.BooleanVar(value=True)
			self.follow_symlink_var = tk.BooleanVar(value=False)
			self.threshold_var = tk.DoubleVar(value=DEFAULT_GROUP_THRESHOLD)
			self.levels_var = tk.IntVar(value=2)

			self._build_layout(ttk)


		def _build_layout(self, ttk_module: object) -> None:
			style = ttk.Style(self)
			style.theme_use("clam")

			# -- Modern dark palette --
			bg = "#0f172a"
			surface = "#1e293b"
			surface2 = "#334155"
			border = "#475569"
			fg = "#f1f5f9"
			fg_dim = "#94a3b8"
			accent = "#3b82f6"
			accent_hover = "#60a5fa"

			style.configure("TFrame", background=bg)
			style.configure("TLabelframe", background=bg, foreground=fg, borderwidth=0, relief="flat")
			style.configure("TLabelframe.Label", background=bg, foreground=accent_hover, font=("Segoe UI Semibold", 10))
			style.configure("TLabel", background=bg, foreground=fg, font=("Segoe UI", 10))
			style.configure("TEntry", fieldbackground=surface2, foreground=fg, insertcolor=fg, font=("Segoe UI", 10), borderwidth=0, relief="flat", padding=(6, 5))
			style.configure("TCheckbutton", background=bg, foreground=fg, font=("Segoe UI", 10))
			style.map("TCheckbutton", background=[("active", bg)])
			style.configure("TSpinbox", fieldbackground=surface2, foreground=fg, arrowcolor=fg, background=surface2, font=("Segoe UI", 10), borderwidth=0, relief="flat")
			style.map("TSpinbox", arrowcolor=[("active", accent_hover)], background=[("active", border)])

			# Buttons
			style.configure(
				"Accent.TButton",
				background=accent,
				foreground="#ffffff",
				font=("Segoe UI Semibold", 10),
				padding=(14, 6),
				borderwidth=0,
			)
			style.map(
				"Accent.TButton",
				background=[("active", accent_hover), ("pressed", "#2563eb")],
			)
			style.configure(
				"TButton",
				background=surface2,
				foreground=fg,
				font=("Segoe UI", 10),
				padding=(12, 5),
				borderwidth=0,
			)
			style.map(
				"TButton",
				background=[("active", border), ("pressed", surface)],
			)

			# Treeview
			style.configure(
				"Treeview",
				rowheight=30,
				fieldbackground=bg,
				background=bg,
				foreground=fg,
				borderwidth=0,
				relief="flat",
				font=("Segoe UI", 10),
			)
			style.configure(
				"Treeview.Heading",
				background=surface2,
				foreground=fg_dim,
				font=("Segoe UI Semibold", 9),
				borderwidth=0,
				relief="flat",
				padding=(8, 4),
			)
			style.map(
				"Treeview",
				background=[("selected", accent)],
				foreground=[("selected", "#ffffff")],
			)
			style.map("Treeview.Heading", background=[("active", border)])
			# Remove treeview outer border
			style.layout("Treeview", [("Treeview.treearea", {"sticky": "nswe"})])

			# Remove Entry/Spinbox inner border and corner pixels for clean look
			style.configure("TEntry", lightcolor=surface2, darkcolor=surface2, bordercolor=surface2, focuscolor=surface2, focusthickness=0)
			style.map("TEntry", lightcolor=[("focus", accent)], bordercolor=[("focus", accent)])
			style.configure("TSpinbox", lightcolor=surface2, darkcolor=surface2, bordercolor=surface2, focuscolor=surface2, focusthickness=0, arrowsize=14)
			style.map("TSpinbox", lightcolor=[("focus", accent)], bordercolor=[("focus", accent)])

			# -- Layout --
			root_frame = ttk_module.Frame(self, padding=16)
			root_frame.pack(fill="both", expand=True)

			controls = ttk_module.Frame(root_frame, padding=(16, 12))
			controls.pack(fill="x")

			ttk_module.Label(controls, text="Path", font=("Segoe UI Semibold", 10)).grid(row=0, column=0, sticky="w")
			path_entry = ttk_module.Entry(controls, textvariable=self.path_var, width=78)
			path_entry.grid(row=0, column=1, columnspan=5, sticky="ew", padx=(10, 10))
			ttk_module.Button(controls, text="Browse", command=self.choose_folder).grid(row=0, column=6, padx=(0, 8))
			ttk_module.Button(controls, text="  Scan  ", command=self.scan_current_path, style="Accent.TButton").grid(row=0, column=7)
			ttk_module.Button(controls, text="Force Rescan", command=self.force_rescan).grid(row=0, column=8, padx=(8, 0))

			nav_frame = ttk_module.Frame(controls)
			nav_frame.grid(row=1, column=0, columnspan=8, sticky="ew", pady=(12, 0))

			ttk_module.Button(nav_frame, text="Back", command=self.go_back).pack(side="left", padx=(0, 6))
			ttk_module.Button(nav_frame, text="Up", command=self.go_up).pack(side="left", padx=(0, 16))

			ttk_module.Checkbutton(
				nav_frame,
				text="Group small items",
				variable=self.group_var,
				command=self.refresh_view,
			).pack(side="left", padx=(0, 12))
			ttk_module.Checkbutton(
				nav_frame,
				text="Follow symlinks",
				variable=self.follow_symlink_var,
			).pack(side="left", padx=(0, 16))

			ttk_module.Label(nav_frame, text="Threshold %", foreground=fg_dim, font=("Segoe UI", 9)).pack(side="left")
			threshold_spin = ttk_module.Spinbox(
				nav_frame,
				from_=1,
				to=25,
				increment=1,
				textvariable=self.threshold_var,
				width=5,
				command=self.refresh_view,
			)
			threshold_spin.pack(side="left", padx=(4, 16))

			ttk_module.Label(nav_frame, text="Levels", foreground=fg_dim, font=("Segoe UI", 9)).pack(side="left")
			ttk_module.Spinbox(
				nav_frame,
				from_=1,
				to=8,
				increment=1,
				textvariable=self.levels_var,
				width=4,
				command=self.refresh_view,
			).pack(side="left", padx=(4, 0))

			controls.columnconfigure(1, weight=1)

			# Breadcrumb bar
			self.breadcrumb_frame = ttk_module.Frame(root_frame)
			self.breadcrumb_frame.pack(fill="x", pady=(14, 0))

			ttk_module.Label(root_frame, textvariable=self.status_var, foreground=fg_dim, font=("Segoe UI", 9)).pack(
				fill="x", pady=(4, 0)
			)

			content = ttk_module.Frame(root_frame)
			content.pack(fill="both", expand=True, pady=(12, 0))

			canvas_frame = ttk_module.Frame(content)
			canvas_frame.pack(side="left", fill="both", expand=True)

			self.canvas = tk.Canvas(
				canvas_frame,
				bg="#0b1120",
				highlightthickness=0,
				relief="flat",
				bd=0,
			)
			self.canvas.pack(fill="both", expand=True)
			self.canvas.bind("<Configure>", lambda _event: self.refresh_view())
			self.canvas.bind("<Button-1>", self.on_canvas_click)
			self.canvas.bind("<Motion>", self.on_canvas_hover)
			self.canvas.bind("<Leave>", self.on_canvas_leave)

			sidebar = ttk_module.Frame(content, padding=(0, 0))
			sidebar.pack(side="right", fill="y", padx=(12, 0))

			# -- Info panel with lighter background --
			style.configure("Info.TFrame", background=surface)
			style.configure("Info.TLabel", background=surface, foreground=fg, font=("Segoe UI", 10))
			style.configure("InfoDim.TLabel", background=surface, foreground=fg_dim, font=("Cascadia Code", 9))
			style.configure("Info.TButton", background=surface2, foreground=fg, font=("Segoe UI", 10), padding=(12, 5), borderwidth=0)

			info_panel = ttk_module.Frame(sidebar, style="Info.TFrame", padding=(14, 14))
			info_panel.pack(fill="x")

			# Section header
			style.configure("InfoTitle.TLabel", background=surface, foreground=accent_hover, font=("Segoe UI Semibold", 10))
			ttk_module.Label(info_panel, text="Selection", style="InfoTitle.TLabel").pack(fill="x", pady=(0, 8))

			# Separator line
			sep_frame = tk.Frame(info_panel, bg="#475569", height=1)
			sep_frame.pack(fill="x", pady=(0, 10))

			self.selection_label = tk.Label(
				info_panel,
				textvariable=self.selection_var,
				wraplength=280,
				justify="left",
				anchor="nw",
				background=surface,
				foreground=fg,
				font=("Segoe UI", 10),
				padx=0,
				pady=4,
				borderwidth=0,
				highlightthickness=0,
			)
			self.selection_label.pack(fill="x")

			action_row = ttk_module.Frame(info_panel, style="Info.TFrame")
			action_row.pack(fill="x", pady=(12, 0))
			ttk_module.Button(action_row, text="Open selected", command=self.open_selected_in_explorer).pack(
				side="left", padx=(0, 6)
			)
			ttk_module.Button(action_row, text="Open folder", command=self.open_current_folder_in_explorer).pack(side="left")

			# Summary separator
			sep_frame2 = tk.Frame(info_panel, bg="#475569", height=1)
			sep_frame2.pack(fill="x", pady=(14, 10))

			style.configure("SummaryTitle.TLabel", background=surface, foreground=accent_hover, font=("Segoe UI Semibold", 10))
			ttk_module.Label(info_panel, text="Summary", style="SummaryTitle.TLabel").pack(fill="x", pady=(0, 6))

			self.summary_label = tk.Label(
				info_panel,
				text="",
				background=surface,
				foreground=fg_dim,
				font=("Segoe UI", 9),
				justify="left",
				anchor="nw",
				wraplength=280,
				padx=0,
				pady=4,
				borderwidth=0,
				highlightthickness=0,
			)
			self.summary_label.pack(fill="x")

			# -- Spacer --
			ttk_module.Frame(sidebar, height=8).pack(fill="x")

			# -- List panel with lighter background --
			list_panel = ttk_module.Frame(sidebar, style="Info.TFrame", padding=(8, 10))
			list_panel.pack(fill="both", expand=True)

			style.configure("ListTitle.TLabel", background=surface, foreground=accent_hover, font=("Segoe UI Semibold", 10))
			ttk_module.Label(list_panel, text="Contents", style="ListTitle.TLabel").pack(fill="x", padx=6, pady=(0, 8))

			self.tree = ttk_module.Treeview(list_panel, columns=("name", "size", "share", "kind"), show="headings", height=18)
			self.tree.heading("name", text="Name")
			self.tree.heading("size", text="Size")
			self.tree.heading("share", text="Share")
			self.tree.heading("kind", text="Type")
			self.tree.column("name", width=180, anchor="w")
			self.tree.column("size", width=100, anchor="e")
			self.tree.column("share", width=70, anchor="e")
			self.tree.column("kind", width=70, anchor="center")
			self.tree.pack(fill="both", expand=True)

			# Update treeview background to match list panel
			style.configure("Treeview", fieldbackground=surface, background=surface)
			self.tree.bind("<Double-1>", self.on_tree_double_click)
			self.tree.bind("<<TreeviewSelect>>", self.on_tree_select)

		def choose_folder(self) -> None:
			"""Open a folder browser dialog."""
			selected = filedialog.askdirectory(title="Choose a folder to scan")
			if selected:
				self.path_var.set(selected)

		def _segment_at(self, x: float, y: float) -> Optional[ChartSegment]:
			"""Return the chart segment at canvas coordinates (x, y)."""
			width = max(self.canvas.winfo_width(), 400)
			height = max(self.canvas.winfo_height(), 400)
			center_x = width / 2
			center_y = height / 2
			for segment in reversed(self.segments):
				if segment.contains_point(x, y, center_x, center_y):
					return segment
			return None

		def _update_summary(self) -> None:
			"""Refresh the summary text panel with current folder stats."""
			if not self.current_node:
				return
			file_count, dir_count = count_descendants(self.current_node)
			lines = [
				f"Path: {self.current_node.path}",
				f"Total size: {format_size(self.current_node.size)}",
				f"Files below current folder: {file_count}",
				f"Subfolders below current folder: {dir_count}",
				f"Immediate children shown: {len(self.display_root.children) if self.display_root else 0}",
				f"Grouping small items: {'On' if self.group_var.get() else 'Off'}",
			]
			if self.current_node.inaccessible_children:
				lines.append(f"Skipped children due to access issues: {self.current_node.inaccessible_children}")
			self._set_summary_text("\n".join(lines))

		def _set_summary_text(self, text: str) -> None:
			"""Replace the summary text content."""
			self.summary_label.configure(text=text)

		def _set_selected_node(self, node: Optional[DisplayNode]) -> None:
			"""Store and describe the current user selection."""

			self.selected_display_node = node
			if node is None:
				self.selection_var.set("Hover over a segment to inspect it.")
				return
			self.selection_var.set(self._describe_node(node))

		def _describe_node(self, node: DisplayNode) -> str:
			"""Return details for tree-selected items."""

			kind = "Grouped items" if node.is_group else ("Folder" if node.is_dir else "File")
			suffix = "Double-click to drill into this folder." if node.is_dir and not node.is_group else ""
			lines = [
				f"{node.name}",
				f"Type: {kind}",
				f"Size: {format_size(node.size)}",
			]
			if self.display_root and self.display_root.size:
				lines.append(f"Share of parent: {node.size / self.display_root.size * 100:.2f}%")
			if suffix:
				lines.append(suffix)
			return "\n".join(lines)

		def _open_path_in_explorer(self, path: Path, *, select: bool = False) -> None:
			"""Open a path in Windows File Explorer, selecting the file when requested."""

			if not path.exists():
				messagebox.showwarning("Storage visualizer", f"Path no longer exists:\n{path}")
				return

			try:
				if os.name == "nt":
					if select and path.is_file():
						subprocess.Popen(["explorer", f"/select,{path}"])
					else:
						os.startfile(str(path))
				elif os.uname().sysname == "Darwin":  # macOS
					if select and path.is_file():
						subprocess.Popen(["open", "-R", str(path)])
					else:
						subprocess.Popen(["open", str(path)])
				else:  # Linux / other
					subprocess.Popen(["xdg-open", str(path)])
			except Exception as exc:  # pragma: no cover - UI dialog path
				messagebox.showerror("Storage visualizer", f"Could not open Explorer:\n{exc}")

		def open_selected_in_explorer(self) -> None:
			"""Open the selected tree/chart item in File Explorer."""

			node = self.selected_display_node
			if not node or not node.source_node:
				messagebox.showinfo("Storage visualizer", "Select a file or folder first.")
				return

			target_path = node.source_node.path
			self._open_path_in_explorer(target_path, select=target_path.is_file())

		def open_current_folder_in_explorer(self) -> None:
			"""Open the currently focused folder in File Explorer."""

			if not self.current_node:
				messagebox.showinfo("Storage visualizer", "No folder is currently loaded.")
				return
			self._open_path_in_explorer(self.current_node.path)

		def scan_current_path(self) -> None:
			raw_path = self.path_var.get().strip()
			if not raw_path:
				messagebox.showinfo("Storage visualizer", "Choose a folder to scan first.")
				return

			self.status_var.set(f"Scanning {raw_path} ...")
			self.update_idletasks()

			import threading

			def run_scan() -> None:
				errors: list[str] = []

				def record_error(path: Path, exc: Exception) -> None:
					errors.append(f"{path}: {exc}")

				try:
					root = scan_directory(
						raw_path,
						follow_symlinks=self.follow_symlink_var.get(),
						on_error=record_error,
					)
				except Exception as exc:
					self.after(0, lambda: messagebox.showerror("Storage visualizer", str(exc)))
					self.after(0, lambda: self.status_var.set("Scan failed."))
					return

				def apply_result() -> None:
					self.root_node = root
					self.current_node = root
					self.history.clear()
					self.path_var.set(str(root.path))

					file_count, dir_count = count_descendants(root)
					suffix = f" ({len(errors)} inaccessible entries skipped)" if errors else ""
					self.status_var.set(
						f"Scanned {root.path} · {format_size(root.size)} across {file_count} files and {dir_count} folders{suffix}"
					)
					self.refresh_view()

				self.after(0, apply_result)

			threading.Thread(target=run_scan, daemon=True).start()

		def force_rescan(self) -> None:
			"""Delete the cache file for the current path and rescan."""
			raw_path = self.path_var.get().strip()
			if not raw_path:
				messagebox.showinfo("Storage visualizer", "Choose a folder first.")
				return
			root = Path(raw_path).expanduser().resolve()
			cache_path = _cache_path_for_scan(root)
			if cache_path.exists():
				try:
					cache_path.unlink()
				except OSError:
					pass
			self.status_var.set(f"Cache cleared. Rescanning {raw_path} ...")
			self.scan_current_path()

		def refresh_view(self) -> None:
			if not self.current_node:
				self.canvas.delete("all")
				self.breadcrumb_var.set("")
				self._update_breadcrumbs()
				self._set_summary_text("No scan loaded yet.")
				self._populate_tree([])
				self._set_selected_node(None)
				return

			if not self.current_node.children_loaded:
				# Load children in a background thread to keep the UI responsive.
				self.status_var.set(f"Loading {self.current_node.name} ...")
				self.update_idletasks()

				import threading
				node = self.current_node

				def load_and_refresh() -> None:
					load_immediate_children(
						node,
						follow_symlinks=self.follow_symlink_var.get(),
					)
					self.after(0, self._finish_refresh)

				threading.Thread(target=load_and_refresh, daemon=True).start()
				return

			self._finish_refresh()

		def _update_breadcrumbs(self) -> None:
			"""Rebuild the breadcrumb bar with clickable labels."""
			import tkinter as tk
			for widget in self.breadcrumb_frame.winfo_children():
				widget.destroy()
			if not self.current_node:
				return
			chain = breadcrumbs(self.current_node)
			for i, node in enumerate(chain):
				if i > 0:
					sep = tk.Label(
						self.breadcrumb_frame, text="  ›  ",
						bg="#0f172a", fg="#475569",
						font=("Segoe UI", 11),
					)
					sep.pack(side="left")
				is_last = (i == len(chain) - 1)
				lbl = tk.Label(
					self.breadcrumb_frame, text=node.name,
					bg="#0f172a",
					fg="#f1f5f9" if is_last else "#60a5fa",
					font=("Segoe UI Semibold", 11, "underline") if not is_last else ("Segoe UI Semibold", 11),
					cursor="hand2" if not is_last else "",
				)
				lbl.pack(side="left")
				if not is_last:
					target = node
					lbl.bind("<Button-1>", lambda _e, t=target: self._navigate_to(t))
					lbl.bind("<Enter>", lambda _e, w=lbl: w.configure(fg="#93c5fd"))
					lbl.bind("<Leave>", lambda _e, w=lbl: w.configure(fg="#60a5fa"))

		def _navigate_to(self, node: StorageNode) -> None:
			"""Navigate to a specific node from the breadcrumb bar."""
			if node is self.current_node:
				return
			self.history.append(self.current_node)
			self.current_node = node
			self.refresh_view()

		def _finish_refresh(self) -> None:
			"""Complete the view refresh after children are loaded."""
			if not self.current_node:
				return

			self.display_root = apply_display_grouping(
				self.current_node,
				min_percent=float(self.threshold_var.get()),
				enabled=self.group_var.get(),
			)
			crumb = "  ›  ".join(node.name for node in breadcrumbs(self.current_node))
			self.breadcrumb_var.set(f"Current folder: {crumb}")
			self._update_breadcrumbs()
			self._draw_chart()
			self._update_summary()
			self._populate_tree(self.display_root.children)
			self._set_selected_node(None)
			file_count, dir_count = count_descendants(self.current_node)
			self.status_var.set(
				f"{self.current_node.path} · {format_size(self.current_node.size)} · {file_count} files · {dir_count} folders"
			)

		def _draw_chart(self) -> None:
			self.canvas.delete("all")
			self.segments = []
			self.hovered_segment = None
			self.segment_styles.clear()

			if not self.display_root:
				return

			width = max(self.canvas.winfo_width(), 400)
			height = max(self.canvas.winfo_height(), 400)
			center_x = width / 2
			center_y = height / 2

			available_radius = min(width, height) / 2 - 34
			max_levels = max(1, int(self.levels_var.get()))
			center_radius = min(110.0, available_radius * 0.30)
			ring_width = max(28.0, (available_radius - center_radius) / max_levels)

			# Subtle background ring improves donut definition when there are few segments.
			outer_radius = center_radius + ring_width * max_levels
			self.canvas.create_oval(
				center_x - outer_radius,
				center_y - outer_radius,
				center_x + outer_radius,
				center_y + outer_radius,
				fill="",
				outline="#1e293b",
				width=2,
			)

			if self.display_root.size > 0 and self.display_root.children:
				self.segments = build_chart_segments(
					self.display_root,
					max_levels=max_levels,
					center_radius=center_radius,
					ring_width=ring_width,
					ring_gap=8.0,
				)

			for segment in self.segments:
				color = "#64748b" if segment.node.is_group else _stable_color(segment.node.name, segment.depth)
				border_color = _mix_color(color, "#0b1120", 0.3)
				points = self._wedge_polygon(
					center_x, center_y,
					segment.inner_radius, segment.outer_radius,
					segment.start_angle, segment.extent,
				)
				segment.item_id = self.canvas.create_polygon(
					points,
					fill=color,
					outline=border_color,
					width=1.5,
					smooth=False,
				)
				if segment.item_id is not None:
					self.segment_styles[segment.item_id] = {
						"base_color": color,
						"hover_color": _hover_color(color),
					}

				if segment.extent >= 13:
					self._draw_label(segment, center_x, center_y)

			self.canvas.create_oval(
				center_x - center_radius,
				center_y - center_radius,
				center_x + center_radius,
				center_y + center_radius,
				fill="#111827",
				outline="#334155",
				width=2,
			)
			root_label = self.current_node.name if self.current_node else "No data"
			self.canvas.create_text(
				center_x,
				center_y - 14,
				text=root_label,
				fill="#e2e8f0",
				font=("Segoe UI", 13, "bold"),
				width=center_radius * 1.65,
			)
			self.canvas.create_text(
				center_x,
				center_y + 18,
				text=format_size(self.display_root.size),
				fill="#93c5fd",
				font=("Segoe UI", 11),
			)

		def _wedge_polygon(
			self,
			cx: float, cy: float,
			inner_r: float, outer_r: float,
			start_angle: float, extent: float,
			steps_per_degree: float = 0.5,
		) -> list[float]:
			"""Return a flat list of (x, y) coords forming a filled wedge polygon."""
			num_steps = max(4, int(abs(extent) * steps_per_degree))
			points: list[float] = []
			# Outer arc from start to end
			for i in range(num_steps + 1):
				angle = math.radians(start_angle + extent * i / num_steps)
				points.append(cx + math.cos(angle) * outer_r)
				points.append(cy - math.sin(angle) * outer_r)
			# Inner arc from end back to start
			for i in range(num_steps, -1, -1):
				angle = math.radians(start_angle + extent * i / num_steps)
				points.append(cx + math.cos(angle) * inner_r)
				points.append(cy - math.sin(angle) * inner_r)
			return points

		def _draw_label(self, segment: ChartSegment, center_x: float, center_y: float) -> None:
			angle = math.radians(segment.start_angle + segment.extent / 2)
			radius = (segment.inner_radius + segment.outer_radius) / 2
			x = center_x + math.cos(angle) * radius
			y = center_y - math.sin(angle) * radius
			label = segment.node.name if len(segment.node.name) <= 18 else f"{segment.node.name[:15]}..."
			segment_color = "#64748b" if segment.node.is_group else _stable_color(segment.node.name, segment.depth)
			self.canvas.create_text(
				x,
				y,
				text=label,
				fill=_label_color_for_segment(segment_color),
				font=("Segoe UI", 9, "bold"),
				width=max(90, int(segment.extent * 2.3)),
			)

		def _populate_tree(self, nodes: Iterable[DisplayNode]) -> None:
			self.tree.delete(*self.tree.get_children())
			self.visible_tree_nodes = list(nodes)
			total = self.display_root.size if self.display_root else 0
			for node in self.visible_tree_nodes:
				percent = node.size / total * 100 if total else 0
				kind = "Group" if node.is_group else ("Folder" if node.is_dir else "File")
				item_id = self.tree.insert("", "end", values=(node.name, format_size(node.size), f"{percent:.1f}%", kind))
				self.tree.item(item_id, tags=(node.name,))

		def _find_depth1_ancestor(self, segment: ChartSegment) -> Optional[ChartSegment]:
			"""For a segment at depth > 1, find the depth-1 segment that contains it."""
			if segment.depth <= 1:
				return segment
			# Walk through segments to find the depth-1 segment whose angle range
			# contains this segment's start angle.
			for candidate in self.segments:
				if candidate.depth != 1:
					continue
				seg_start = candidate.start_angle % 360
				seg_end = (candidate.start_angle + candidate.extent) % 360
				point = segment.start_angle % 360
				# Handle wrap-around
				if seg_start <= seg_end:
					if seg_start <= point <= seg_end:
						return candidate
				else:
					if point >= seg_start or point <= seg_end:
						return candidate
			return segment

		def on_canvas_click(self, event: tk.Event) -> None:
			segment = self._segment_at(event.x, event.y)
			if not segment:
				return

			self._set_selected_node(segment.node)
			self._hide_tooltip()

			# Show grouped items in a popup window
			if segment.node.is_group:
				self._show_others_popup(segment.node)
				return

			# For depth > 1, navigate to the depth-1 parent so the hierarchy
			# shifts by one level rather than jumping directly to the deep node.
			target = self._find_depth1_ancestor(segment) if segment.depth > 1 else segment

			navigable = target.node.source_node
			if target.node.is_group or not navigable or not navigable.is_dir:
				return

			self.history.append(self.current_node)
			self.current_node = navigable
			self.refresh_view()

		def _show_others_popup(self, node: DisplayNode) -> None:
			"""Show a popup listing all items grouped under 'Others'."""
			popup = tk.Toplevel(self)
			popup.title(f"Others — {len(node.children)} grouped items ({format_size(node.size)})")
			popup.geometry("520x420")
			popup.configure(bg="#0f172a")
			popup.transient(self)
			popup.grab_set()

			header = tk.Label(
				popup,
				text=f'Items grouped under "Others" ({format_size(node.size)})',
				bg="#0f172a",
				fg="#60a5fa",
				font=("Segoe UI Semibold", 11),
				pady=10,
			)
			header.pack(fill="x", padx=16)

			tree = ttk.Treeview(popup, columns=("name", "size", "kind"), show="headings", height=16)
			tree.heading("name", text="Name")
			tree.heading("size", text="Size")
			tree.heading("kind", text="Type")
			tree.column("name", width=280, anchor="w")
			tree.column("size", width=120, anchor="e")
			tree.column("kind", width=80, anchor="center")
			tree.pack(fill="both", expand=True, padx=16, pady=(0, 8))

			items = sorted(node.children, key=lambda c: -c.size)
			for child in items:
				kind = "Folder" if child.is_dir else "File"
				tree.insert("", "end", values=(child.name, format_size(child.size), kind))

			def on_double_click(_event: tk.Event) -> None:
				selected = tree.selection()
				if not selected:
					return
				idx = tree.index(selected[0])
				if idx >= len(items):
					return
				child = items[idx]
				if child.source_node and child.source_node.is_dir:
					popup.destroy()
					self.history.append(self.current_node)
					self.current_node = child.source_node
					self.refresh_view()

			tree.bind("<Double-1>", on_double_click)

			btn = ttk.Button(popup, text="Close", command=popup.destroy)
			btn.pack(pady=(0, 12))

		def on_tree_select(self, _event: tk.Event) -> None:
			selected = self.tree.selection()
			if not selected:
				self._set_selected_node(None)
				return

			index = self.tree.index(selected[0])
			if 0 <= index < len(self.visible_tree_nodes):
				self._set_selected_node(self.visible_tree_nodes[index])

		def on_tree_double_click(self, _event: tk.Event) -> None:
			if not self.display_root:
				return

			selected = self.tree.selection()
			if not selected:
				return

			index = self.tree.index(selected[0])
			if index >= len(self.visible_tree_nodes):
				return

			node = self.visible_tree_nodes[index]
			self._set_selected_node(node)
			if node.is_group or not node.source_node or not node.source_node.is_dir:
				return

			self.history.append(self.current_node)
			self.current_node = node.source_node
			self.refresh_view()

		def on_canvas_hover(self, event: tk.Event) -> None:
			segment = self._segment_at(event.x, event.y)
			if segment is self.hovered_segment:
				# Update tooltip position even if same segment
				if segment and hasattr(self, '_tooltip'):
					self._move_tooltip(event.x_root, event.y_root)
				return

			if self.hovered_segment and self.hovered_segment.item_id:
				style = self.segment_styles.get(self.hovered_segment.item_id)
				if style:
					self.canvas.itemconfigure(
						self.hovered_segment.item_id,
						fill=style["base_color"],
					)

			self.hovered_segment = segment
			self._hide_tooltip()

			if segment and segment.item_id:
				style = self.segment_styles.get(segment.item_id)
				if style:
					self.canvas.itemconfigure(
						segment.item_id,
						fill=style["hover_color"],
					)
				self._show_tooltip(event.x_root, event.y_root, self._describe_segment(segment))

		def on_canvas_leave(self, _event: tk.Event) -> None:
			if self.hovered_segment and self.hovered_segment.item_id:
				style = self.segment_styles.get(self.hovered_segment.item_id)
				if style:
					self.canvas.itemconfigure(
						self.hovered_segment.item_id,
						fill=style["base_color"],
					)
			self.hovered_segment = None
			self._hide_tooltip()

		def _show_tooltip(self, x: int, y: int, text: str) -> None:
			"""Display a floating tooltip near the cursor."""
			self._hide_tooltip()
			tw = tk.Toplevel(self)
			tw.wm_overrideredirect(True)
			tw.wm_attributes("-topmost", True)
			tw.configure(bg="#1e293b")
			# Position offset from cursor
			tw.wm_geometry(f"+{x + 16}+{y + 12}")

			frame = tk.Frame(tw, bg="#1e293b", padx=10, pady=8, highlightbackground="#475569", highlightthickness=1)
			frame.pack()
			label = tk.Label(
				frame,
				text=text,
				justify="left",
				bg="#1e293b",
				fg="#f1f5f9",
				font=("Segoe UI", 10),
				wraplength=300,
			)
			label.pack()
			self._tooltip = tw

		def _move_tooltip(self, x: int, y: int) -> None:
			"""Reposition the existing tooltip."""
			if hasattr(self, '_tooltip') and self._tooltip:
				try:
					self._tooltip.wm_geometry(f"+{x + 16}+{y + 12}")
				except tk.TclError:
					self._tooltip = None

		def _hide_tooltip(self) -> None:
			"""Destroy the tooltip window if it exists."""
			tw = getattr(self, '_tooltip', None)
			if tw:
				try:
					tw.destroy()
				except tk.TclError:
					pass
				self._tooltip = None

		def _describe_segment(self, segment: ChartSegment) -> str:
			kind = "Grouped items" if segment.node.is_group else ("Folder" if segment.node.is_dir else "File")
			if segment.node.is_group:
				suffix = "Click to see grouped items."
			elif segment.node.is_dir:
				suffix = "Click to drill into this folder."
			else:
				suffix = ""
			lines = [
				f"{segment.node.name}",
				f"Type: {kind}",
				f"Size: {format_size(segment.node.size)}",
				f"Share of parent: {segment.percent_of_parent:.2f}%",
			]
			if segment.node.is_group and segment.node.children:
				items = sorted(segment.node.children, key=lambda c: -c.size)[:8]
				lines.append(f"Contains {len(segment.node.children)} items:")
				for child in items:
					icon = "📁" if child.is_dir else "📄"
					lines.append(f"  {icon} {child.name} ({format_size(child.size)})")
				if len(segment.node.children) > 8:
					lines.append(f"  … and {len(segment.node.children) - 8} more")
			if suffix:
				lines.append(suffix)
			return "\n".join(lines)

		def go_back(self) -> None:
			if not self.history:
				return
			self.current_node = self.history.pop()
			self.refresh_view()

		def go_up(self) -> None:
			if not self.current_node or not self.current_node.parent:
				return
			self.history.append(self.current_node)
			self.current_node = self.current_node.parent
			self.refresh_view()

	app = StorageVisualizerApp()
	app.mainloop()


def main() -> None:
	"""Launch the desktop UI."""
	launch_app()


if __name__ == "__main__":
	main()
