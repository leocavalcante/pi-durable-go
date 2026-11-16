#!/usr/bin/env python3
"""Draws .github/social-preview.png, the repository's social preview.

Needs Pillow and Go: the text is set in the Go fonts, from the
golang.org/x/image module go.mod requires. Upload the result in the
repository's settings, under Social preview.

    python3 scripts/social-preview.py
"""

import os
import subprocess

from PIL import Image, ImageDraw, ImageFilter, ImageFont

W, H = 1280, 640
S = 2  # drawn at twice the size, then reduced, for smooth edges

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, ".github", "social-preview.png")
FONTS = os.path.join(
    subprocess.check_output(
        ["go", "list", "-m", "-f", "{{.Dir}}", "golang.org/x/image"], cwd=ROOT, text=True
    ).strip(),
    "font",
    "gofont",
    "ttfs",
)

GO_BLUE = (0, 173, 216)
WHITE = (248, 250, 252)
LIGHT = (203, 213, 225)
MUTED = (148, 163, 184)
FAINT = (100, 116, 139)
LINE = (51, 65, 85)
CRASH = (249, 115, 22)
DONE = (34, 197, 94)


def font(name, size):
    return ImageFont.truetype(os.path.join(FONTS, name), size * S)


def p(*values):
    """Scales coordinates to the drawing."""
    return tuple(v * S for v in values)


def background():
    top, bottom = (10, 15, 30), (17, 28, 52)
    image = Image.new("RGB", p(W, H))
    draw = ImageDraw.Draw(image)
    for y in range(H * S):
        t = y / (H * S - 1)
        draw.line([(0, y), (W * S, y)], fill=tuple(round(a + (b - a) * t) for a, b in zip(top, bottom)))
    # A soft Go-blue glow at the top right.
    glow = Image.new("RGBA", p(W, H), (0, 0, 0, 0))
    ImageDraw.Draw(glow).ellipse(p(820, -260, 1480, 360), fill=GO_BLUE + (70,))
    glow = glow.filter(ImageFilter.GaussianBlur(140 * S))
    image = Image.alpha_composite(image.convert("RGBA"), glow)
    # A faint grid of dots.
    dots = Image.new("RGBA", p(W, H), (0, 0, 0, 0))
    draw = ImageDraw.Draw(dots)
    for x in range(40, W, 40):
        for y in range(40, H, 40):
            draw.ellipse(p(x - 1, y - 1, x + 1, y + 1), fill=(148, 163, 184, 28))
    return Image.alpha_composite(image, dots)


def spaced(draw, xy, text, face, fill, spacing):
    """Draws text with extra space between its letters."""
    x, y = xy
    for char in text:
        draw.text((x * S, y * S), char, font=face, fill=fill)
        x += face.getlength(char) / S + spacing


def title(draw, xy, text, size, fill):
    """Draws text in Go Bold with narrower hyphens: the font draws its hyphen
    wide, to serve as a minus sign in code."""
    face = font("Go-Bold.ttf", size)
    x, y = xy
    for i, part in enumerate(text.split("-")):
        if i:
            # Go Bold's hyphen: 0.61 to 0.705 em below the origin.
            bearing, width = 0.07 * size, 0.30 * size
            draw.rectangle(p(x + bearing, y + 0.61 * size, x + bearing + width, y + 0.705 * size), fill=fill)
            x += 2 * bearing + width
        draw.text((x * S, y * S), part, font=face, fill=fill)
        x += face.getlength(part) / S


def dot(draw, x, y, r, fill, ring=None):
    if ring:
        draw.ellipse(p(x - r - 5, y - r - 5, x + r + 5, y + r + 5), fill=ring)
    draw.ellipse(p(x - r, y - r, x + r, y + r), fill=fill)


def label(draw, x, y, text, face, fill):
    width = face.getlength(text) / S
    draw.text(((x - width / 2) * S, y * S), text, font=face, fill=fill)


def main():
    image = background()
    draw = ImageDraw.Draw(image)
    left = 88

    spaced(draw, (left, 92), "A GO PORT OF PI DURABLE", font("Go-Medium.ttf", 22), GO_BLUE, 3)
    title(draw, (left - 4, 124), "pi-durable-go", 100, WHITE)
    draw.text(p(left, 262), "Durable AI agents for Go", font=font("Go-Medium.ttf", 44), fill=LIGHT)
    body = font("Go-Regular.ttf", 28)
    draw.text(p(left, 332), "Conversations, tool calls, and tasks that survive", font=body, fill=MUTED)
    draw.text(p(left, 372), "crashes, restarts, and deploys.", font=body, fill=MUTED)

    # A run's timeline: checkpoints, a crash, and the run resuming to its end,
    # spread so the first label starts at the text and the end meets the margin.
    y, mono = 478, font("Go-Mono.ttf", 18)
    ring = (13, 20, 38)
    xs = [142 + k * (1170 - 142) / 8 for k in range(9)]
    checkpoints, cx, resumed, ex = xs[:4], xs[4], xs[5:8], xs[8]
    draw.line(p(checkpoints[0], y, cx, y), fill=LINE, width=4 * S)
    draw.line(p(cx, y, ex, y), fill=(22, 101, 52), width=4 * S)
    for x in checkpoints:
        dot(draw, x, y, 10, GO_BLUE, ring)
    label(draw, checkpoints[0], y + 26, "checkpoint", mono, FAINT)
    # The crash: an orange ring with a cross.
    dot(draw, cx, y, 17, CRASH, ring)
    dot(draw, cx, y, 12, (40, 22, 12))
    for a, b in (((-6, -6), (6, 6)), ((-6, 6), (6, -6))):
        draw.line(p(cx + a[0], y + a[1], cx + b[0], y + b[1]), fill=CRASH, width=4 * S)
    label(draw, cx, y + 26, "crash", mono, CRASH)
    for x in resumed:
        dot(draw, x, y, 10, DONE, ring)
    label(draw, resumed[0], y + 26, "resume", mono, FAINT)
    # The end: a green disc with a check.
    dot(draw, ex, y, 17, DONE, ring)
    draw.line(p(ex - 8, y + 1, ex - 2, y + 7, ex + 9, y - 6), fill=(10, 15, 30), width=4 * S, joint="curve")
    label(draw, ex, y + 26, "done", mono, DONE)

    footer = font("Go-Mono.ttf", 20)
    draw.text(p(left, 572), "github.com/leocavalcante/pi-durable-go", font=footer, fill=FAINT)
    right = "MIT · Go 1.26+"
    draw.text(((1192 - footer.getlength(right) / S) * S, 572 * S), right, font=footer, fill=FAINT)

    image = image.convert("RGB").resize((W, H), Image.LANCZOS)
    image.save(OUT, optimize=True)
    print(f"{os.path.relpath(OUT, ROOT)}: {W}x{H}, {os.path.getsize(OUT)} bytes")


if __name__ == "__main__":
    main()
