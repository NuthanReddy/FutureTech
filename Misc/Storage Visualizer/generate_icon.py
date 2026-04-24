"""Generate a storage visualizer icon as .ico file using only stdlib.

Run once:  python "Misc/Storage Visualizer/generate_icon.py"
Produces:  Misc/Storage Visualizer/icon.ico
"""

import struct
import math
import zlib
from pathlib import Path


def _create_png(size: int) -> bytes:
    """Create a PNG image of a donut chart icon at the given size."""
    cx, cy = size / 2, size / 2
    outer_r = size * 0.44
    inner_r = size * 0.22
    bg = (15, 23, 42, 0)  # transparent

    # Segment colors (RGBA)
    segments = [
        (0, 90, (56, 189, 248, 255)),    # blue
        (90, 180, (168, 85, 247, 255)),   # purple
        (180, 270, (52, 211, 153, 255)),  # green
        (270, 360, (251, 146, 60, 255)),  # orange
    ]

    pixels = []
    for y in range(size):
        row = []
        for x in range(size):
            dx, dy = x - cx, cy - y
            r = math.hypot(dx, dy)
            if inner_r <= r <= outer_r:
                angle = math.degrees(math.atan2(dy, dx))
                if angle < 0:
                    angle += 360
                color = bg
                for start, end, c in segments:
                    if start <= angle < end:
                        color = c
                        break
                # Add slight gap between segments
                for start, _, _ in segments:
                    delta = abs(((angle - start + 180) % 360) - 180)
                    if delta < 2.5:
                        color = bg
                        break
                row.append(color)
            elif r < inner_r:
                # Center circle
                row.append((17, 24, 39, 255) if r < inner_r - 1 else bg)
            else:
                row.append(bg)
        pixels.append(row)

    # Encode as PNG
    def _make_png(pixels: list, width: int, height: int) -> bytes:
        def _chunk(chunk_type: bytes, data: bytes) -> bytes:
            c = chunk_type + data
            crc = zlib.crc32(c) & 0xFFFFFFFF
            return struct.pack(">I", len(data)) + c + struct.pack(">I", crc)

        raw = b""
        for row in pixels:
            raw += b"\x00"  # filter: none
            for r, g, b, a in row:
                raw += struct.pack("BBBB", r, g, b, a)

        ihdr = struct.pack(">IIBBBBB", width, height, 8, 6, 0, 0, 0)
        compressed = zlib.compress(raw, 9)

        png = b"\x89PNG\r\n\x1a\n"
        png += _chunk(b"IHDR", ihdr)
        png += _chunk(b"IDAT", compressed)
        png += _chunk(b"IEND", b"")
        return png

    return _make_png(pixels, size, size)


def create_ico(output_path: Path) -> None:
    """Generate a multi-size .ico file."""
    sizes = [16, 32, 48, 256]
    images = []
    for s in sizes:
        images.append((s, _create_png(s)))

    # ICO format
    num = len(images)
    header = struct.pack("<HHH", 0, 1, num)
    offset = 6 + num * 16
    entries = b""
    data = b""

    for size, png_data in images:
        w = 0 if size == 256 else size
        h = 0 if size == 256 else size
        entries += struct.pack("<BBBBHHII", w, h, 0, 0, 1, 32, len(png_data), offset)
        data += png_data
        offset += len(png_data)

    output_path.write_bytes(header + entries + data)
    print(f"Icon written to {output_path} ({len(header + entries + data)} bytes)")


if __name__ == "__main__":
    create_ico(Path(__file__).parent / "icon.ico")

