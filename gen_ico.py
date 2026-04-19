"""Generate app.ico — proper Windows ICO (PNG-in-ICO format, Vista+).
Icon matches the system tray icon: blue circle #4361ee with white ring.
Run directly or called by 一键打包.bat before PyInstaller.
"""
import os
import struct
from io import BytesIO

from PIL import Image, ImageDraw


def _draw_frame(size: int) -> Image.Image:
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    m = max(1, size // 32)
    # Outer blue circle
    draw.ellipse([m, m, size - m - 1, size - m - 1], fill="#4361ee")
    # White ring
    inner = size * 18 // 64
    outer_inner = size - size * 19 // 64
    if outer_inner > inner:
        draw.ellipse([inner, inner, outer_inner, outer_inner], fill="#ffffff")
    # Center blue dot
    cd = size * 28 // 64
    cd2 = size - size * 29 // 64
    if cd2 > cd:
        draw.ellipse([cd, cd, cd2, cd2], fill="#4361ee")
    return img


def generate(output: str = "app.ico"):
    sizes = [16, 24, 32, 48, 64, 128, 256]

    # Render each size and compress as PNG
    png_bufs: list[bytes] = []
    for s in sizes:
        buf = BytesIO()
        _draw_frame(s).save(buf, format="PNG")
        png_bufs.append(buf.getvalue())

    count = len(sizes)
    # ICO header: reserved=0, type=1 (ICO), image count
    header = struct.pack("<HHH", 0, 1, count)

    # Directory entries (16 bytes each)
    offset = 6 + count * 16
    dir_entries = b""
    for s, data in zip(sizes, png_bufs):
        w = s if s < 256 else 0   # 0 = 256 in ICO spec
        h = s if s < 256 else 0
        dir_entries += struct.pack("<BBBBHHII", w, h, 0, 0, 1, 32, len(data), offset)
        offset += len(data)

    with open(output, "wb") as f:
        f.write(header)
        f.write(dir_entries)
        for data in png_bufs:
            f.write(data)

    print(f"[gen_ico] {output} generated — {os.path.getsize(output)} bytes, {count} sizes: {sizes}")


if __name__ == "__main__":
    generate()
