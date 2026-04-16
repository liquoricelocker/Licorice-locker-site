#!/usr/bin/env python3
"""
Batch-apply a Kodak Portra 400–inspired look (approximation) to product images.

This does NOT replace a proper Lightroom/Capture One + LUT workflow. It is a
consistent, automatable baseline: warmer mids, slight magenta lean, lifted
shadows, softer contrast, muted saturation, fine grain, optional vignette.

Rules respected by design:
- Same dimensions and framing as input (no crop, no geometry change).
- No object removal/addition (pixel operations only).

Review outputs before publishing: very strong grades can shift product colour.
Use --strength < 1.0 to stay safer on colour-critical SKUs.

Examples:
  python scripts/portra_film_batch.py -i ./static -o ./static/film-out --ext .png .jpg .webp
  python scripts/portra_film_batch.py -i photo.png -o graded.png
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from PIL import Image, ImageChops, ImageDraw, ImageEnhance, ImageFilter, ImageOps


def _has_effect_noise() -> bool:
    return hasattr(Image, "effect_noise")


def _fine_grain_layer(size: tuple[int, int], sigma: float) -> Image.Image:
    if _has_effect_noise():
        n = Image.effect_noise(size, sigma)  # type: ignore[attr-defined]
        if n.mode == "L":
            n = Image.merge("RGB", (n, n, n))
        elif n.mode != "RGB":
            n = n.convert("RGB")
        return ImageEnhance.Brightness(n).enhance(0.45)

    w, h = size
    raw = os.urandom(w * h)
    g = Image.frombytes("L", (w, h), raw)
    g = Image.merge("RGB", (g, g, g))
    return ImageEnhance.Brightness(g).enhance(0.2)


def _vignette_mask(size: tuple[int, int], strength: float) -> Image.Image:
    """Radial falloff: brighter in center, darker at edges (as alpha toward darkening)."""
    w, h = size
    s = max(128, min(384, min(w, h) // 2))
    m = Image.new("L", (s, s), 0)
    d = ImageDraw.Draw(m)
    pad = s // 16
    d.ellipse((pad, pad, s - pad, s - pad), fill=255)
    m = m.filter(ImageFilter.GaussianBlur(radius=s // 10))
    m = ImageOps.invert(m)
    m = m.point(lambda p: int(p * strength))
    return m.resize((w, h), Image.Resampling.LANCZOS)


def portra_grade(
    im: Image.Image,
    *,
    strength: float = 1.0,
    grain_amount: float = 0.038,
    blur_radius: float = 0.38,
    vignette: float = 0.14,
) -> Image.Image:
    """Return a new RGB image; input may be RGBA (flattened on white)."""
    if im.mode in ("RGBA", "P"):
        background = Image.new("RGB", im.size, (255, 255, 255))
        if im.mode == "P":
            im = im.convert("RGBA")
        background.paste(im, mask=im.split()[-1] if im.mode == "RGBA" else None)
        im = background
    else:
        im = im.convert("RGB")

    st = max(0.2, min(1.5, strength))

    # Slight softness (reduce digital crispness)
    r_blur = blur_radius * st
    if r_blur > 0.05:
        im = im.filter(ImageFilter.GaussianBlur(radius=r_blur))

    # Warm + subtle magenta in shadows/mids (channel lift)
    r, g, b = im.split()
    r = r.point(lambda v: min(255, int(v * (1.0 + 0.035 * st) + 5 * st)))
    g = g.point(lambda v: min(255, int(v * (1.0 + 0.012 * st) + 2 * st)))
    b = b.point(lambda v: max(0, int(v * (1.0 - 0.055 * st) - 4 * st)))
    im = Image.merge("RGB", (r, g, b))

    # Tonal: lower contrast, gentle highlight compression feel, lifted blacks
    im = ImageEnhance.Contrast(im).enhance(0.82 + 0.08 / st)
    im = ImageEnhance.Brightness(im).enhance(1.0 + 0.025 * st)
    im = ImageEnhance.Color(im).enhance(0.86 - 0.04 * (st - 1.0))  # muted sat

    # Fine grain (screen-style lift on texture)
    g_amt = grain_amount * st
    if g_amt > 0.005:
        grain = _fine_grain_layer(im.size, sigma=10 + 6 * st)
        im = Image.blend(im, ImageChops.screen(im, grain), g_amt)

    # Subtle vignette (darken edges slightly)
    if vignette > 0.01:
        vw = vignette * st
        mask = _vignette_mask(im.size, strength=0.55)
        dark = Image.new("RGB", im.size, (32, 26, 28))
        edge = Image.composite(dark, im, mask)
        im = Image.blend(im, edge, vw)

    return im


def _output_path_for(input_path: Path, output: Path, flat: bool) -> Path:
    if output.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp"}:
        return output
    output.mkdir(parents=True, exist_ok=True)
    if flat:
        return output / f"{input_path.stem}_portra{input_path.suffix}"
    rel = input_path.name
    return output / f"{Path(rel).stem}_portra{Path(rel).suffix}"


def main() -> int:
    ap = argparse.ArgumentParser(description="Portra-style film batch (Pillow approximation).")
    ap.add_argument("-i", "--input", required=True, help="File or directory")
    ap.add_argument("-o", "--output", required=True, help="File or directory")
    ap.add_argument(
        "--ext",
        nargs="*",
        default=[".png", ".jpg", ".jpeg", ".webp"],
        help="Extensions to process when input is a directory",
    )
    ap.add_argument("--strength", type=float, default=1.0, help="Overall grade intensity (0.3–1.3 typical)")
    ap.add_argument("--grain", type=float, default=0.038)
    ap.add_argument("--blur", type=float, default=0.38)
    ap.add_argument("--vignette", type=float, default=0.14)
    ap.add_argument("--flat", action="store_true", help="Write files as name_portra.ext into output dir")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    inp = Path(args.input).expanduser().resolve()
    out = Path(args.output).expanduser().resolve()
    exts = {e.lower() if e.startswith(".") else f".{e.lower()}" for e in args.ext}

    files: list[Path] = []
    if inp.is_file():
        files = [inp]
    elif inp.is_dir():
        for e in sorted(exts):
            files.extend(inp.rglob(f"*{e}"))
    else:
        print("Input path not found.", file=sys.stderr)
        return 1

    if not files:
        print("No matching files.", file=sys.stderr)
        return 1

    for f in files:
        if f.name.startswith("."):
            continue
        dest = _output_path_for(f, out, args.flat)
        if args.dry_run:
            print(f"{f} -> {dest}")
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            img = Image.open(f)
            graded = portra_grade(
                img,
                strength=args.strength,
                grain_amount=args.grain,
                blur_radius=args.blur,
                vignette=args.vignette,
            )
            save_kw: dict = {}
            suf = dest.suffix.lower()
            if suf in {".jpg", ".jpeg"}:
                save_kw["quality"] = 95
                save_kw["optimize"] = True
            elif suf == ".webp":
                save_kw["quality"] = 92
                save_kw["method"] = 6
            elif suf == ".png":
                save_kw["optimize"] = True

            graded.save(dest, **save_kw)
            print(f"Wrote {dest}")
        except OSError as exc:
            print(f"Skip {f}: {exc}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
