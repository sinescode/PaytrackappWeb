"""
card_generator.py — Python/Pillow port of Flutter's card_generator.dart
Card size: 960 × 480  (2:1 landscape, same as Flutter)

Font strategy (in order of preference):
  1. Fonts in the same folder as this file (drop Monoton-Regular.ttf next to card_generator.py)
  2. System FreeSans / FreeSansBold (Linux servers)
  3. PIL default (last resort — ৳ may not render)
"""

import io
import os
from PIL import Image, ImageDraw, ImageFont

# ── Font resolution ───────────────────────────────────────────────────────────

# Directory where THIS file lives — put custom .ttf files here
_HERE = os.path.dirname(os.path.abspath(__file__))

def _find_font(bold: bool = False, mono: bool = False) -> str | None:
    """
    Return an absolute path to a usable TTF font, or None (PIL default).
    Priority:
      1. Monoton-Regular.ttf next to this file (user's custom font)
      2. FreeSansBold / FreeSans  (Linux system font with ৳ glyph)
      3. None → PIL default bitmap font
    """
    candidates = []

    # 1. Custom font in same directory
    candidates.append(os.path.join(_HERE, "LobsterTwo-Regular.ttf"))

    # 2. System FreeFont (has ৳ U+09F3)
    free_dir = "/usr/share/fonts/truetype/freefont/"
    if bold and not mono:
        candidates.append(os.path.join(free_dir, "FreeSansBold.ttf"))
    elif mono and bold:
        candidates.append(os.path.join(free_dir, "FreeMonoBold.ttf"))
    elif mono:
        candidates.append(os.path.join(free_dir, "FreeMono.ttf"))
    else:
        candidates.append(os.path.join(free_dir, "FreeSans.ttf"))

    for path in candidates:
        if os.path.isfile(path):
            return path
    return None


def _font(bold: bool = False, size: int = 14, mono: bool = False) -> ImageFont.FreeTypeFont:
    path = _find_font(bold=bold, mono=mono)
    if path:
        return ImageFont.truetype(path, size)
    # Last resort: PIL default (no ৳ but won't crash)
    return ImageFont.load_default()


# ── Palette ───────────────────────────────────────────────────────────────────
BG_DARK       = (10,  15,  30)
BG_CARD       = (17,  24,  39)
ACCENT_CREDIT = (0,   245, 160)
ACCENT_DEBIT  = (255, 77,  109)
SURFACE_LIGHT = (30,  42,  58)
TEXT_PRIMARY  = (241, 245, 249)
TEXT_MUTED    = (100, 116, 139)
DIVIDER       = (30,  41,  59)


def _alpha_blend(color, alpha: float, bg=BG_CARD):
    return tuple(int(c * alpha + b * (1 - alpha)) for c, b in zip(color, bg))


def generate_card(user_id: str, display_name: str, pending: float, date: str) -> bytes:
    W, H = 960, 480

    is_credit  = pending < 0
    abs_pend   = abs(pending)
    abs_str    = f"{abs_pend:.2f}"
    initial    = (display_name[0] if display_name else user_id[0]).upper()
    accent     = ACCENT_CREDIT if is_credit else ACCENT_DEBIT
    status_lbl = "CREDIT" if is_credit else "DEBIT"
    bal_label  = "CREDIT BALANCE" if is_credit else "PENDING BALANCE"

    accent_dim = _alpha_blend(accent, 0.12)
    accent_25  = _alpha_blend(accent, 0.25)
    accent_55  = _alpha_blend(accent, 0.55)
    accent_70  = _alpha_blend(accent, 0.70)
    accent_08  = _alpha_blend(accent, 0.08)

    img  = Image.new("RGB", (W, H), BG_CARD)
    draw = ImageDraw.Draw(img)

    # 1. Gradient background
    for y in range(H):
        t = y / H
        draw.line([(0, y), (W, y)], fill=(
            int(BG_DARK[0] * (1-t) + 13*t),
            int(BG_DARK[1] * (1-t) + 27*t),
            int(BG_DARK[2] * (1-t) + 42*t),
        ))

    # 2. Glow orb
    orb = Image.new("RGB", (W, H), (0, 0, 0))
    od  = ImageDraw.Draw(orb)
    cx, cy, rad = 120, 100, 220
    for r in range(rad, 0, -2):
        col = _alpha_blend(accent, 0.18 * (1 - r/rad), (0,0,0))
        od.ellipse([cx-r, cy-r, cx+r, cy+r], fill=col)
    img  = Image.blend(img, orb, alpha=0.6)
    draw = ImageDraw.Draw(img)

    # 3. Grid
    gc = _alpha_blend(DIVIDER, 0.5)
    for gx in range(48, W, 48): draw.line([(gx,0),(gx,H)], fill=gc)
    for gy in range(48, H, 48): draw.line([(0,gy),(W,gy)], fill=gc)

    # 4. Border
    draw.rounded_rectangle([1,1,W-2,H-2], radius=32, outline=accent_25, width=2)

    # 5. Top stripe
    draw.polygon([(0,0),(W*0.55,0),(W*0.45,4),(0,4)], fill=accent)

    # 6. Avatar
    avX, avY, avS, avR = 48, 48, 80, 16
    for g in range(12, 0, -1):
        draw.rounded_rectangle(
            [avX-4-g, avY-4-g, avX+avS+4+g, avY+avS+4+g],
            radius=avR+4+g, outline=_alpha_blend(accent, 0.20*(1-g/12)), width=1)
    draw.rounded_rectangle([avX, avY, avX+avS, avY+avS], radius=avR,
                            fill=_alpha_blend(accent, 0.65))
    f_init = _font(bold=True, size=34)
    bb = draw.textbbox((0,0), initial, font=f_init)
    draw.text((avX+avS//2-(bb[2]-bb[0])//2, avY+avS//2-(bb[3]-bb[1])//2-2),
              initial, font=f_init, fill=BG_DARK)

    # 7. Name & ID
    nameX = avX + avS + 24
    draw.text((nameX, avY+4),  display_name,     font=_font(bold=True,  size=24), fill=TEXT_PRIMARY)
    draw.text((nameX, avY+38), user_id.upper(),  font=_font(bold=False, size=12), fill=TEXT_MUTED)

    # 8. PAYTRACK logo
    lf  = _font(bold=True, size=12)
    lbb = draw.textbbox((0,0), "PAYTRACK", font=lf)
    draw.text((W-48-(lbb[2]-lbb[0]), avY+24), "PAYTRACK", font=lf, fill=accent_55)

    # 9. Divider
    divY = 168
    draw.line([(48, divY), (W-48, divY)], fill=DIVIDER)

    # 10. Balance
    balLabelY = divY + 28
    balAmtY   = balLabelY + 24
    draw.text((48, balLabelY), bal_label, font=_font(bold=True, size=10), fill=TEXT_MUTED)
    draw.text((48, balAmtY+10), "৳", font=_font(bold=False, size=30), fill=accent_70)
    draw.text((92, balAmtY),   abs_str,  font=_font(bold=True, size=68, mono=True), fill=accent)

    # 11. Status pill
    pillH, pillY = 40, H-52
    pf   = _font(bold=True, size=12)
    pbb  = draw.textbbox((0,0), status_lbl, font=pf)
    pillW = 16 + 10 + 10 + (pbb[2]-pbb[0]) + 20
    draw.rounded_rectangle([48, pillY, 48+pillW, pillY+pillH], radius=20, fill=accent_dim)
    draw.rounded_rectangle([48, pillY, 48+pillW, pillY+pillH], radius=20, outline=accent_25, width=1)
    dot_cx, dot_cy = 48+16+5, pillY+pillH//2
    draw.ellipse([dot_cx-5, dot_cy-5, dot_cx+5, dot_cy+5], fill=accent)
    for g in range(6,0,-1):
        draw.ellipse([dot_cx-5-g, dot_cy-5-g, dot_cx+5+g, dot_cy+5+g],
                     outline=_alpha_blend(accent, 0.3*(1-g/6)))
    draw.text((48+16+10+10, pillY+pillH//2-8), status_lbl, font=pf, fill=accent)

    # 12. Date bottom-right
    df  = _font(bold=False, size=12)
    dbb = draw.textbbox((0,0), date, font=df)
    draw.text((W-48-(dbb[2]-dbb[0]), pillY+pillH//2-7), date, font=df, fill=TEXT_MUTED)

    # 13. Right panel
    panelX, panelY_p = int(W*0.62), 48
    panelW, panelH_p = W-int(W*0.62)-32, H-96
    draw.rounded_rectangle([panelX, panelY_p, panelX+panelW, panelY_p+panelH_p],
                            radius=20, fill=SURFACE_LIGHT)
    draw.rounded_rectangle([panelX, panelY_p, panelX+panelW, panelY_p+panelH_p],
                            radius=20, outline=accent_08, width=1)

    # Mini bar chart
    bars  = [0.4,0.65,0.5,0.8,0.55,0.9,0.7,0.85]
    cw    = panelW - 32
    bw    = (cw - (len(bars)-1)*4) / len(bars)
    for i, bv in enumerate(bars):
        bx = panelX+16 + i*(bw+4)
        bh = bv*80
        by = panelY_p+16+80-bh
        draw.rounded_rectangle([bx,by,bx+bw,by+bh], radius=4,
                                fill=accent if i==len(bars)-1 else _alpha_blend(accent,0.25))

    # Stat rows
    row_y = panelY_p + 112
    lbl_f = _font(bold=True,  size=10)
    val_f = _font(bold=True,  size=14)
    for lbl, val in [("DATE",date),("TYPE","Credit" if is_credit else "Debit"),("AMOUNT",f"৳{abs_str}")]:
        draw.text((panelX+16, row_y),    lbl, font=lbl_f, fill=(71,85,105))
        draw.text((panelX+16, row_y+16), val, font=val_f, fill=(203,213,225))
        row_y += 58
        if row_y < panelY_p+panelH_p-30:
            draw.line([(panelX+16,row_y-12),(panelX+panelW-16,row_y-12)], fill=DIVIDER)

    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=False)
    return buf.getvalue()
