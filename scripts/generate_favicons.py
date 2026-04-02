#!/usr/bin/env python3
"""Stdlib-only: bar-chart favicon -> PNG + ICO (PNG içinde) for eski/yeni tarayıcılar."""
from __future__ import annotations

import struct
import zlib
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
STATIC = BASE_DIR / "static"

# Arka plan ve bar renkleri (SVG ile uyumlu)
BG = (21, 34, 56)
BARS = [
    (5, 19, 5.5, 8, (14, 165, 233)),
    (13.25, 13, 5.5, 14, (37, 99, 235)),
    (21.5, 7, 5.5, 20, (13, 148, 136)),
]
LINE = ((5, 24.5), (12, 17), (18.5, 19.5), (27, 9))


def _line_pixels(ax: float, ay: float, bx: float, by: float, size: int) -> set[tuple[int, int]]:
    """Bresenham benzeri ince çizgi (1px)."""
    pts: set[tuple[int, int]] = set()
    x0, y0 = int(round(ax * size / 32)), int(round(ay * size / 32))
    x1, y1 = int(round(bx * size / 32)), int(round(by * size / 32))
    dx, dy = abs(x1 - x0), abs(y1 - y0)
    sx, sy = (1 if x0 < x1 else -1), (1 if y0 < y1 else -1)
    err = dx - dy
    x, y = x0, y0
    while True:
        if 0 <= x < size and 0 <= y < size:
            pts.add((x, y))
        if x == x1 and y == y1:
            break
        e2 = 2 * err
        if e2 > -dy:
            err -= dy
            x += sx
        if e2 < dx:
            err += dx
            y += sy
    return pts


def rasterize(size: int) -> bytes:
    buf = bytearray(size * size * 4)
    s = size / 32.0

    def put(px: int, py: int, r: int, g: int, b: int, a: int = 255) -> None:
        if 0 <= px < size and 0 <= py < size:
            i = (py * size + px) * 4
            buf[i : i + 4] = bytes([r, g, b, a])

    for yy in range(size):
        for xx in range(size):
            put(xx, yy, *BG)

    for bx, by, bw, bh, col in BARS:
        x0 = int(bx * s)
        y0 = int(by * s)
        x1 = int((bx + bw) * s) + 1
        y1 = int((by + bh) * s) + 1
        for yy in range(max(0, y0), min(size, y1)):
            for xx in range(max(0, x0), min(size, x1)):
                put(xx, yy, *col)

    line_pts: set[tuple[int, int]] = set()
    for i in range(len(LINE) - 1):
        (ax, ay), (bx, by) = LINE[i], LINE[i + 1]
        line_pts |= _line_pixels(ax, ay, bx, by, size)
    for (lx, ly) in line_pts:
        r, g, b = 255, 255, 255
        put(lx, ly, r, g, b, 90)

    return bytes(buf)


def write_png(path: Path, width: int, height: int, rgba: bytes) -> None:
    assert len(rgba) == width * height * 4
    sig = b"\x89PNG\r\n\x1a\n"

    def chunk(tag: bytes, data: bytes) -> bytes:
        crc = zlib.crc32(tag + data) & 0xFFFFFFFF
        return struct.pack(">I", len(data)) + tag + data + struct.pack(">I", crc)

    ihdr = struct.pack(">IIBBBBB", width, height, 8, 6, 0, 0, 0)
    raw = b""
    for y in range(height):
        raw += b"\x00" + rgba[y * width * 4 : (y + 1) * width * 4]
    compressed = zlib.compress(raw, 9)
    png = sig + chunk(b"IHDR", ihdr) + chunk(b"IDAT", compressed) + chunk(b"IEND", b"")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(png)


def _png_dimensions(png: bytes) -> tuple[int, int]:
    if len(png) < 24 or png[:8] != b"\x89PNG\r\n\x1a\n":
        return 32, 32
    if png[12:16] != b"IHDR":
        return 32, 32
    return struct.unpack(">II", png[16:24])


def write_ico(path: Path, png_chunks: list[bytes]) -> None:
    """ICO: birden fazla PNG (Windows Vista+ ve modern tarayıcılar)."""
    count = len(png_chunks)
    header = struct.pack("<HHH", 0, 1, count)
    base_offset = 6 + 16 * count
    entries: list[bytes] = []
    off = base_offset
    for png in png_chunks:
        w, h = _png_dimensions(png)
        bw = 0 if w >= 256 else w
        bh = 0 if h >= 256 else h
        entries.append(
            struct.pack("<BBBBHHII", bw, bh, 0, 0, 1, 0, len(png), off)
        )
        off += len(png)
    path.write_bytes(header + b"".join(entries) + b"".join(png_chunks))


def main() -> None:
    STATIC.mkdir(parents=True, exist_ok=True)
    sizes = [16, 32, 180]
    png_by_size: dict[int, bytes] = {}
    for sz in sizes:
        rgba = rasterize(sz)
        png_path = STATIC / (f"favicon-{sz}.png" if sz != 180 else "apple-touch-icon.png")
        write_png(png_path, sz, sz, rgba)
        png_by_size[sz] = png_path.read_bytes()

    # favicon.ico: 16 + 32 (klasik sekme)
    write_ico(STATIC / "favicon.ico", [png_by_size[16], png_by_size[32]])

    # Kök /static için genel isim
    write_png(STATIC / "favicon.png", 32, 32, rasterize(32))

    print("OK:", STATIC)


if __name__ == "__main__":
    main()
