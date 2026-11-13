import FreeSimpleGUI as psg
import sys
import os
import io
import math
import numpy as np
import pandas as pd
import multiprocessing as mp
import time
import threading
import tkinter as tk
from tkinter import ttk
from functools import partial
from math import ceil
from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageTk


# =====================================================================
# PATH & RESOURCE MANAGEMENT (Cross-Platform & PyInstaller Ready)
# =====================================================================
def resource_path(relative_path):
    """Get absolute path to resource, works for dev and for PyInstaller."""
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)


for _sub in (
    "fragment_extractor",
    "feature_extractor",
    os.path.join("feature_extractor", "NIST"),
    os.path.join("feature_extractor", "HEDGE_specials"),
    os.path.join("feature_extractor", "BLFCP_specials"),
    os.path.join("feature_extractor", "SOTA_specials"),
    os.path.join("feature_extractor", "AFRL_specials"),
    "combine_csv",
):
    sys.path.append(resource_path(_sub))

from extract_whole_folder_fragments import extract_whole_folder_fragments
from extract_each_file_fragments import extract_random_fragments_file, extract_random_fragments_file_random_pad
from nist_suite import (
    longest_run_ones_in_a_block_test, overlapping_template_matching_test,
    approximate_entropy_test, frequency_within_block_test, runs_test,
    random_excursion_variant_test, maurers_universal_test, cumulative_sums_test,
    non_overlapping_template_matching_test, monobit_test, random_excursion_test,
    dft_test, serial_test, binary_matrix_rank_test, linear_complexity_test,
)
from HEDGE_fspecials import chi2_uniform, chi_test_absolute, chi_test_confidence, HEDGE_test
from BLFCP_fspecials import (
    byte_frequency_distribution, discrete_fourier_transform,
    lempel_ziv_complexity, monte_carlo_pi_approximation,
)
from SOTA_fspecials import (
    byte_statistical_moments, ks_uniformity_test, wasserstein_uniform_distance,
    index_of_coincidence, generalized_entropies, serial_and_lag_autocorrelation,
    markov_bigram_features, byte_difference_features, recompression_ratios,
    haar_wavelet_detail_ratio, spectral_flatness_measure, structural_run_and_ascii,
    SOTA_test,
)
from AFRL_fspecials import local_nibbles_variance, AFRL_test
from combine import concatenate_rows_gui, concatenate_columns_gui


# =====================================================================
# SOLID OBSIDIAN-SLATE UI PALETTE + FROSTED GLASS BUTTON ENGINE
# =====================================================================
BG_MAIN        = '#090D16'
BG_CARD        = '#111827'
BG_CARD_TOP    = '#151F32'
BG_INPUT       = '#0B101B'
BORDER_CARD    = '#1F2D42'
BORDER_INPUT   = '#26364F'
TEXT_PRIMARY   = '#F8FAFC'
TEXT_MUTED     = '#94A3B8'
ACCENT_EMERALD = '#10B981'
ACCENT_CYAN    = '#38BDF8'
ACCENT_AMBER   = '#F59E0B'

psg.LOOK_AND_FEEL_TABLE['ObsidianGlassButtons'] = {
    'BACKGROUND': BG_MAIN,
    'TEXT': TEXT_PRIMARY,
    'INPUT': BG_INPUT,
    'TEXT_INPUT': TEXT_PRIMARY,
    'SCROLL': '#2A3B58',
    'BUTTON': (TEXT_PRIMARY, BG_CARD),
    'PROGRESS': (ACCENT_EMERALD, '#1E293B'),
    'BORDER': 0,
    'SLIDER_DEPTH': 0,
    'PROGRESS_DEPTH': 0,
}

psg.theme('ObsidianGlassButtons')
psg.set_options(
    font=('Segoe UI', 10),
    button_color=(TEXT_PRIMARY, BG_CARD),
    border_width=0,
    element_padding=(6, 5),
    margins=(14, 14)
)

FONT_TITLE    = ('Segoe UI Semibold', 15)
FONT_SUBTITLE = ('Segoe UI', 9)
FONT_SECTION  = ('Segoe UI Semibold', 10)
FONT_BOLD     = ('Segoe UI Semibold', 10)
FONT_MONO     = ('Consolas', 9)


def _hex_to_rgb(hex_color: str):
    h = hex_color.lstrip('#')
    return tuple(int(h[i:i + 2], 16) for i in (0, 2, 4))


def _load_ui_font(size: int, bold: bool = True):
    candidates = [
        "seguisb.ttf" if bold else "segoeui.ttf",
        "segoeuib.ttf" if bold else "segoeui.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
    ]
    for path in candidates:
        try:
            return ImageFont.truetype(path, size=size)
        except Exception:
            continue
    return ImageFont.load_default(size=size)


class GlassButtonRenderer:
    """
    Renders anti-aliased Frosted Glass pill buttons and crisp vector Radio/Checkbox indicators.
    """
    PALETTES = {
        "emerald": {
            "normal":   {"tint_top": (52, 211, 153, 88),   "tint_bot": (5, 150, 105, 122),  "rim": (110, 231, 183, 200), "sheen": 62, "glow": (16, 185, 129, 58),  "text": (255, 255, 255, 255)},
            "hover":    {"tint_top": (110, 231, 183, 130), "tint_bot": (16, 185, 129, 162), "rim": (167, 243, 208, 250), "sheen": 86, "glow": (52, 211, 153, 110), "text": (255, 255, 255, 255)},
            "disabled": {"tint_top": (71, 85, 105, 32),    "tint_bot": (51, 65, 85, 42),    "rim": (100, 116, 139, 70),  "sheen": 16, "glow": (0, 0, 0, 0),        "text": (148, 163, 184, 140)},
        },
        "indigo": {
            "normal":   {"tint_top": (129, 140, 248, 84),  "tint_bot": (79, 70, 229, 116),  "rim": (165, 180, 252, 190), "sheen": 56, "glow": (99, 102, 241, 52),  "text": (255, 255, 255, 255)},
            "hover":    {"tint_top": (165, 180, 252, 124), "tint_bot": (99, 102, 241, 154), "rim": (199, 210, 254, 245), "sheen": 82, "glow": (129, 140, 248, 100),"text": (255, 255, 255, 255)},
            "disabled": {"tint_top": (71, 85, 105, 32),    "tint_bot": (51, 65, 85, 42),    "rim": (100, 116, 139, 70),  "sheen": 16, "glow": (0, 0, 0, 0),        "text": (148, 163, 184, 140)},
        },
        "cyan": {
            "normal":   {"tint_top": (56, 189, 248, 82),   "tint_bot": (2, 132, 199, 114),  "rim": (125, 211, 252, 190), "sheen": 56, "glow": (14, 165, 233, 52),  "text": (255, 255, 255, 255)},
            "hover":    {"tint_top": (125, 211, 252, 124), "tint_bot": (14, 165, 233, 152), "rim": (186, 230, 253, 245), "sheen": 82, "glow": (56, 189, 248, 100), "text": (255, 255, 255, 255)},
            "disabled": {"tint_top": (71, 85, 105, 32),    "tint_bot": (51, 65, 85, 42),    "rim": (100, 116, 139, 70),  "sheen": 16, "glow": (0, 0, 0, 0),        "text": (148, 163, 184, 140)},
        },
        "smoked": {
            "normal":   {"tint_top": (148, 163, 184, 44),  "tint_bot": (51, 65, 85, 68),    "rim": (148, 163, 184, 120), "sheen": 42, "glow": (15, 23, 42, 48),    "text": (241, 245, 249, 255)},
            "hover":    {"tint_top": (148, 163, 184, 78),  "tint_bot": (71, 85, 105, 102),  "rim": (125, 211, 252, 210), "sheen": 64, "glow": (56, 189, 248, 60),  "text": (255, 255, 255, 255)},
            "disabled": {"tint_top": (71, 85, 105, 24),    "tint_bot": (51, 65, 85, 34),    "rim": (100, 116, 139, 58),  "sheen": 14, "glow": (0, 0, 0, 0),        "text": (148, 163, 184, 130)},
        },
        "ruby": {
            "normal":   {"tint_top": (248, 113, 113, 84),  "tint_bot": (220, 38, 38, 118),  "rim": (252, 165, 165, 190), "sheen": 54, "glow": (239, 68, 68, 52),   "text": (255, 255, 255, 255)},
            "hover":    {"tint_top": (252, 165, 165, 124), "tint_bot": (239, 68, 68, 154),  "rim": (254, 202, 202, 245), "sheen": 78, "glow": (248, 113, 113, 100),"text": (255, 255, 255, 255)},
            "disabled": {"tint_top": (71, 85, 105, 28),    "tint_bot": (51, 65, 85, 38),    "rim": (100, 116, 139, 62),  "sheen": 14, "glow": (0, 0, 0, 0),        "text": (148, 163, 184, 130)},
        },
    }

    def __init__(self):
        self._cache = {}
        self._widget_cache = {}

    def _draw_vector_icon(self, draw, icon_type, cx, cy, r, color):
        lw = max(2, int(r * 0.24))
        if icon_type == "play":
            draw.polygon([(cx - r * 0.45, cy - r * 0.65), (cx + r * 0.68, cy), (cx - r * 0.45, cy + r * 0.65)], fill=color)
        elif icon_type == "folder":
            draw.rounded_rectangle([cx - r * 0.75, cy - r * 0.45, cx + r * 0.75, cy + r * 0.6], radius=int(r * 0.2), outline=color, width=lw)
            draw.line([(cx - r * 0.75, cy - r * 0.18), (cx + r * 0.75, cy - r * 0.18)], fill=color, width=lw)
        elif icon_type == "save":
            draw.rounded_rectangle([cx - r * 0.65, cy - r * 0.65, cx + r * 0.65, cy + r * 0.65], radius=int(r * 0.18), outline=color, width=lw)
            draw.rectangle([cx - r * 0.34, cy - r * 0.65, cx + r * 0.34, cy - r * 0.14], fill=color)
        elif icon_type == "bolt":
            draw.polygon([
                (cx + r * 0.1, cy - r * 0.8), (cx - r * 0.45, cy + r * 0.05), (cx - r * 0.02, cy + r * 0.05),
                (cx - r * 0.15, cy + r * 0.8), (cx + r * 0.45, cy - r * 0.05), (cx + r * 0.02, cy - r * 0.05)
            ], fill=color)
        elif icon_type == "shield":
            draw.polygon([
                (cx, cy - r * 0.78), (cx + r * 0.68, cy - r * 0.48), (cx + r * 0.56, cy + r * 0.22),
                (cx, cy + r * 0.80), (cx - r * 0.56, cy + r * 0.22), (cx - r * 0.68, cy - r * 0.48)
            ], outline=color, width=lw)
            draw.line([(cx - r * 0.26, cy + r * 0.02), (cx - r * 0.04, cy + r * 0.24), (cx + r * 0.32, cy - r * 0.20)], fill=color, width=lw)
        elif icon_type == "link":
            draw.arc([cx - r * 0.75, cy - r * 0.35, cx + r * 0.12, cy + r * 0.35], 45, 315, fill=color, width=lw)
            draw.arc([cx - r * 0.12, cy - r * 0.35, cx + r * 0.75, cy + r * 0.35], 225, 135, fill=color, width=lw)
        elif icon_type == "star":
            pts = [
                (cx + (r * 0.76 if i % 2 == 0 else r * 0.34) * math.cos(-math.pi / 2 + i * math.pi / 5),
                 cy + (r * 0.76 if i % 2 == 0 else r * 0.34) * math.sin(-math.pi / 2 + i * math.pi / 5))
                for i in range(10)
            ]
            draw.polygon(pts, fill=color)
        elif icon_type == "check":
            draw.line([(cx - r * 0.6, cy + r * 0.05), (cx - r * 0.15, cy + r * 0.52), (cx + r * 0.65, cy - r * 0.45)], fill=color, width=lw + 1)
        elif icon_type == "close":
            draw.line([(cx - r * 0.5, cy - r * 0.5), (cx + r * 0.5, cy + r * 0.5)], fill=color, width=lw + 1)
            draw.line([(cx + r * 0.5, cy - r * 0.5), (cx - r * 0.5, cy + r * 0.5)], fill=color, width=lw + 1)
        elif icon_type == "sliders":
            for dy, kx in [(-0.42, -0.25), (0.0, 0.25), (0.42, -0.1)]:
                y = cy + r * dy
                draw.line([(cx - r * 0.7, y), (cx + r * 0.7, y)], fill=color, width=lw)
                draw.ellipse([cx + r * kx - lw * 1.35, y - lw * 1.35, cx + r * kx + lw * 1.35, y + lw * 1.35], fill=color)
        elif icon_type == "book":
            draw.rounded_rectangle([cx - r * 0.65, cy - r * 0.65, cx + r * 0.65, cy + r * 0.65], radius=int(r * 0.18), outline=color, width=lw)
            draw.line([(cx, cy - r * 0.65), (cx, cy + r * 0.65)], fill=color, width=lw)
        elif icon_type == "info":
            draw.ellipse([cx - r * 0.68, cy - r * 0.68, cx + r * 0.68, cy + r * 0.68], outline=color, width=lw)
            draw.line([(cx, cy - r * 0.1), (cx, cy + r * 0.4)], fill=color, width=lw + 1)
            draw.ellipse([cx - lw * 0.75, cy - r * 0.42 - lw * 0.75, cx + lw * 0.75, cy - r * 0.42 + lw * 0.75], fill=color)

    def render(self, text: str, width: int, height: int, palette: str = "emerald",
               state: str = "normal", parent_bg: str = BG_CARD, radius: int = 11,
               font_size: int = 13, icon: str = None) -> bytes:
        cache_key = (text, width, height, palette, state, parent_bg, radius, font_size, icon)
        if cache_key in self._cache:
            return self._cache[cache_key]

        spec = self.PALETTES[palette][state]
        scale = 2
        W, H = width * scale, height * scale
        pad, R = 3 * scale, radius * scale
        base = Image.new("RGBA", (W, H), (*_hex_to_rgb(parent_bg), 255))

        if spec["glow"][3] > 0:
            glow_layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
            ImageDraw.Draw(glow_layer).rounded_rectangle([pad, pad + scale, W - pad, H - pad + scale], radius=R, fill=spec["glow"])
            base = Image.alpha_composite(base, glow_layer.filter(ImageFilter.GaussianBlur(radius=3 * scale)))

        mask = Image.new("L", (W, H), 0)
        ImageDraw.Draw(mask).rounded_rectangle([pad, pad, W - pad, H - pad], radius=R, fill=255)

        inner_w, inner_h = max(1, W - 2 * pad), max(1, H - 2 * pad)
        strip = Image.new("RGBA", (1, inner_h), (0, 0, 0, 0))
        t_top, t_bot = spec["tint_top"], spec["tint_bot"]
        strip.putdata([
            tuple(int(t_top[c] + (t_bot[c] - t_top[c]) * (y / float(max(1, inner_h - 1)))) for c in range(4))
            for y in range(inner_h)
        ])
        grad_full = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        grad_full.paste(strip.resize((inner_w, inner_h), Image.Resampling.BILINEAR), (pad, pad))
        glass_body = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        glass_body.paste(grad_full, (0, 0), mask)
        base = Image.alpha_composite(base, glass_body)

        sheen_layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        ImageDraw.Draw(sheen_layer).rounded_rectangle(
            [pad + scale, pad + scale, W - pad - scale, pad + int(inner_h * 0.46)],
            radius=max(2, R - scale), fill=(255, 255, 255, spec["sheen"])
        )
        sheen_masked = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        sheen_masked.paste(sheen_layer, (0, 0), mask)
        base = Image.alpha_composite(base, sheen_masked)

        rim_layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        r_draw = ImageDraw.Draw(rim_layer)
        r_draw.rounded_rectangle([pad, pad, W - pad, H - pad], radius=R, outline=spec["rim"], width=scale)
        r_draw.rounded_rectangle(
            [pad + scale, pad + scale, W - pad - scale, H - pad - scale],
            radius=max(2, R - scale), outline=(255, 255, 255, spec["sheen"] // 2), width=max(1, scale // 2)
        )
        base = Image.alpha_composite(base, rim_layer)

        txt_layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        t_draw = ImageDraw.Draw(txt_layer)
        font = _load_ui_font(font_size * scale, bold=True)
        bbox = t_draw.textbbox((0, 0), text, font=font)
        tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
        icon_r = int(font_size * scale * 0.60)
        gap = int(8 * scale) if icon else 0
        icon_w = (icon_r * 2 + gap) if icon else 0
        start_x = (W - (icon_w + tw)) / 2.0
        cy = H / 2.0
        if icon:
            self._draw_vector_icon(t_draw, icon, start_x + icon_r, cy, icon_r, spec["text"])
        tx, ty = start_x + icon_w, cy - th / 2.0 - bbox[1]
        t_draw.text((tx, ty + scale), text, font=font, fill=(0, 0, 0, 95))
        t_draw.text((tx, ty), text, font=font, fill=spec["text"])
        base = Image.alpha_composite(base, txt_layer)

        buf = io.BytesIO()
        base.resize((width, height), Image.Resampling.LANCZOS).save(buf, format="PNG")
        png_bytes = buf.getvalue()
        self._cache[cache_key] = png_bytes
        return png_bytes

    def render_radio(self, selected: bool, hover: bool = False, bg_hex: str = BG_CARD, size: int = 20) -> ImageTk.PhotoImage:
        key = ("radio", selected, hover, bg_hex, size)
        if key in self._widget_cache:
            return self._widget_cache[key]
        scale = 4
        S = size * scale
        img = Image.new("RGBA", (S, S), (*_hex_to_rgb(bg_hex), 255))
        draw = ImageDraw.Draw(img)
        pad, cx = 2 * scale, S / 2.0
        r_outer = cx - pad
        if selected:
            ring_col = (110, 231, 183, 255) if hover else (52, 211, 153, 255)
            draw.ellipse([cx - r_outer, cx - r_outer, cx + r_outer, cx + r_outer], fill=(16, 185, 129, 48), outline=ring_col, width=int(2.2 * scale))
            r_inner = r_outer * 0.46
            draw.ellipse([cx - r_inner, cx - r_inner, cx + r_inner, cx + r_inner], fill=(52, 211, 153, 255))
        else:
            rim_col = (56, 189, 248, 235) if hover else (75, 100, 138, 215)
            fill_col = (24, 36, 58, 255) if hover else (13, 20, 34, 255)
            draw.ellipse([cx - r_outer, cx - r_outer, cx + r_outer, cx + r_outer], fill=fill_col, outline=rim_col, width=int(1.8 * scale))
        photo = ImageTk.PhotoImage(img.resize((size, size), Image.Resampling.LANCZOS))
        self._widget_cache[key] = photo
        return photo

    def render_checkbox(self, checked: bool, hover: bool = False, bg_hex: str = BG_CARD, size: int = 19) -> ImageTk.PhotoImage:
        key = ("checkbox", checked, hover, bg_hex, size)
        if key in self._widget_cache:
            return self._widget_cache[key]
        scale = 4
        S = size * scale
        img = Image.new("RGBA", (S, S), (*_hex_to_rgb(bg_hex), 255))
        draw = ImageDraw.Draw(img)
        pad, R = 2 * scale, 5 * scale
        if checked:
            fill_col = (52, 211, 153, 255) if hover else (16, 185, 129, 238)
            rim_col = (167, 243, 208, 255) if hover else (110, 231, 183, 255)
            draw.rounded_rectangle([pad, pad, S - pad, S - pad], radius=R, fill=fill_col, outline=rim_col, width=int(1.6 * scale))
            draw.line([(S * 0.27, S * 0.52), (S * 0.44, S * 0.69), (S * 0.74, S * 0.34)], fill=(255, 255, 255, 255), width=int(2.3 * scale), joint="curve")
        else:
            fill_col = (24, 36, 58, 255) if hover else (13, 20, 34, 255)
            rim_col = (56, 189, 248, 235) if hover else (75, 100, 138, 215)
            draw.rounded_rectangle([pad, pad, S - pad, S - pad], radius=R, fill=fill_col, outline=rim_col, width=int(1.6 * scale))
        photo = ImageTk.PhotoImage(img.resize((size, size), Image.Resampling.LANCZOS))
        self._widget_cache[key] = photo
        return photo


GLASS = GlassButtonRenderer()


def make_glass_btn(registry: dict, key: str, text: str, width: int, height: int,
                   palette: str = "emerald", parent_bg: str = BG_CARD, radius: int = 11,
                   font_size: int = 13, icon: str = None, disabled: bool = False,
                   pad=(4, 4), btn_kind: str = "button", file_types=(("CSV Files", "*.csv"),)):
    imgs = {
        st: GLASS.render(text, width, height, palette, st, parent_bg, radius, font_size, icon)
        for st in ("normal", "hover", "disabled")
    }
    registry[key] = {"imgs": imgs, "disabled": disabled, "palette": palette}
    kw = dict(
        key=key,
        image_data=imgs["disabled" if disabled else "normal"],
        button_color=(parent_bg, parent_bg),
        border_width=0,
        pad=pad,
        disabled=disabled,
    )
    if btn_kind == "folder":
        return psg.FolderBrowse("", **kw)
    if btn_kind == "file":
        return psg.FileBrowse("", file_types=file_types, **kw)
    if btn_kind == "save":
        return psg.FileSaveAs("", file_types=file_types, **kw)
    return psg.Button("", **kw)


class SleekCpuSlider:
    """
    Custom high-contrast Canvas slider for CPU Allocation:
      - Recessed dark-obsidian track with crisp 1px border
      - Luminous emerald-to-cyan active progress fill up to the knob
      - High-contrast glowing white/emerald circular thumb with dark drop-shadow ring
      - Click-anywhere + smooth drag + live percentage & core count readout
    """
    def __init__(self, canvas_elem, badge_elem, state_dict: dict, slider_key: str,
                 min_val: int = 10, max_val: int = 100, step: int = 5,
                 default_val: int = 80, bg_hex: str = BG_CARD):
        self.canvas = canvas_elem.Widget
        self.badge = badge_elem
        self.state_dict = state_dict
        self.slider_key = slider_key
        self.min_val = min_val
        self.max_val = max_val
        self.step = step
        self.bg_hex = bg_hex
        self.hover = False
        self.dragging = False
        self.value = int(default_val)
        self.state_dict[self.slider_key] = self.value

        self.canvas.configure(
            bg=bg_hex,
            highlightthickness=0,
            bd=0,
            height=30,
            cursor='hand2',
        )
        self.canvas.bind('<Configure>', lambda _e: self.redraw(), add='+')
        self.canvas.bind('<Enter>', self._on_enter, add='+')
        self.canvas.bind('<Leave>', self._on_leave, add='+')
        self.canvas.bind('<Button-1>', self._on_press, add='+')
        self.canvas.bind('<B1-Motion>', self._on_drag, add='+')
        self.canvas.bind('<ButtonRelease-1>', self._on_release, add='+')
        self._update_badge()

    def _on_enter(self, _ev):
        self.hover = True
        self.redraw()

    def _on_leave(self, _ev):
        self.hover = False
        self.redraw()

    def _on_press(self, ev):
        self.dragging = True
        self._set_from_x(ev.x)

    def _on_drag(self, ev):
        if self.dragging:
            self._set_from_x(ev.x)

    def _on_release(self, _ev):
        self.dragging = False
        self.redraw()

    def _set_from_x(self, x: int):
        w = max(60, self.canvas.winfo_width())
        pad_x = 14
        usable = max(1, w - 2 * pad_x)
        ratio = max(0.0, min(1.0, (x - pad_x) / float(usable)))
        raw_val = self.min_val + ratio * (self.max_val - self.min_val)
        snapped = int(round(raw_val / self.step) * self.step)
        snapped = max(self.min_val, min(self.max_val, snapped))
        if snapped != self.value:
            self.value = snapped
            self.state_dict[self.slider_key] = self.value
            self._update_badge()
        self.redraw()

    def _update_badge(self):
        total_cores = mp.cpu_count()
        used_cores = max(1, int(total_cores * self.value / 100))
        self.badge.update(f"  {self.value:3d}%  │  {used_cores}/{total_cores} Cores  ")

    def _draw_pill(self, x1, y1, x2, y2, fill, outline="", width=1):
        r = max(1, (y2 - y1) / 2.0)
        if x2 - x1 < 2 * r:
            x2 = x1 + 2 * r
        self.canvas.create_oval(x1, y1, x1 + 2 * r, y2, fill=fill, outline=outline, width=width)
        self.canvas.create_oval(x2 - 2 * r, y1, x2, y2, fill=fill, outline=outline, width=width)
        self.canvas.create_rectangle(x1 + r, y1, x2 - r, y2, fill=fill, outline="")
        if outline:
            self.canvas.create_line(x1 + r, y1, x2 - r, y1, fill=outline, width=width)
            self.canvas.create_line(x1 + r, y2, x2 - r, y2, fill=outline, width=width)

    def redraw(self):
        c = self.canvas
        c.delete("all")
        w = c.winfo_width()
        h = c.winfo_height()
        if w < 30:
            w = 380
        if h < 16:
            h = 30

        pad_x = 14
        cy = h // 2
        track_h = 8
        y1 = cy - track_h // 2
        y2 = y1 + track_h
        x_start = pad_x
        x_end = w - pad_x
        usable = max(1, x_end - x_start)

        ratio = (self.value - self.min_val) / float(self.max_val - self.min_val)
        knob_x = x_start + ratio * usable

        self._draw_pill(x_start, y1, x_end, y2, fill="#060A12", outline="#334B70", width=1)

        for tick_pct in (10, 25, 50, 75, 100):
            tr = (tick_pct - self.min_val) / float(self.max_val - self.min_val)
            tx = x_start + tr * usable
            c.create_line(tx, y2 + 3, tx, y2 + 6, fill="#3B5278", width=1)

        if knob_x > x_start + 2:
            fill_col = ACCENT_CYAN if (self.hover or self.dragging) else ACCENT_EMERALD
            rim_col = "#A7F3D0" if not (self.hover or self.dragging) else "#BAE6FD"
            self._draw_pill(x_start, y1, knob_x, y2, fill=fill_col, outline=rim_col, width=1)

        r_halo = 11 if (self.hover or self.dragging) else 10
        r_knob = 8.5 if (self.hover or self.dragging) else 7.5
        r_core = 3.2

        c.create_oval(
            knob_x - r_halo, cy - r_halo, knob_x + r_halo, cy + r_halo,
            fill="#060A12", outline="#38BDF8" if (self.hover or self.dragging) else "#1E293B", width=1
        )
        c.create_oval(
            knob_x - r_knob, cy - r_knob, knob_x + r_knob, cy + r_knob,
            fill="#38BDF8" if (self.hover or self.dragging) else "#10B981",
            outline="#FFFFFF", width=2
        )
        c.create_oval(
            knob_x - r_core, cy - r_core, knob_x + r_core, cy + r_core,
            fill="#FFFFFF", outline=""
        )


def make_cpu_slider_row(slider_key: str, badge_key: str, default_val: int = 80, bg_hex: str = BG_CARD):
    """Builds a high-contrast custom Canvas CPU Allocation slider row + pill readout badge."""
    total_cores = mp.cpu_count()
    init_cores = max(1, int(total_cores * default_val / 100))
    return [
        psg.Text("CPU Allocation:", size=(16, 1), background_color=bg_hex, font=FONT_BOLD),
        psg.Canvas(size=(360, 30), key=slider_key, background_color=bg_hex, expand_x=True, pad=((4, 8), (2, 2))),
        psg.Text(
            f"  {default_val:3d}%  │  {init_cores}/{total_cores} Cores  ",
            key=badge_key,
            size=(21, 1),
            justification='center',
            font=FONT_BOLD,
            text_color='#A7F3D0',
            background_color='#0B192C',
            relief=psg.RELIEF_FLAT,
            pad=((4, 4), (4, 4)),
        ),
    ]


def _configure_modern_scrollbars_and_sliders(root_widget):
    try:
        style = ttk.Style(root_widget)
        if 'clam' in style.theme_names():
            style.theme_use('clam')

        style.layout('Sleek.Vertical.TScrollbar', [
            ('Vertical.Scrollbar.trough', {'sticky': 'ns', 'children': [('Vertical.Scrollbar.thumb', {'expand': '1', 'sticky': 'nswe'})]})
        ])
        style.configure('Sleek.Vertical.TScrollbar', gripcount=0, background='#2A3B58', darkcolor='#2A3B58', lightcolor='#2A3B58', troughcolor='#0B101B', bordercolor='#0B101B', width=9)
        style.map('Sleek.Vertical.TScrollbar', background=[('pressed', ACCENT_EMERALD), ('active', ACCENT_CYAN)])
    except Exception:
        pass

    def _walk(widget):
        try:
            if isinstance(widget, ttk.Scrollbar) and str(widget.cget('orient')) == 'vertical':
                widget.configure(style='Sleek.Vertical.TScrollbar')
            elif isinstance(widget, tk.Scrollbar):
                widget.configure(bg='#2A3B58', troughcolor='#0B101B', activebackground=ACCENT_CYAN, highlightthickness=0, bd=0, width=9, relief='flat')
            elif isinstance(widget, tk.Canvas):
                def _wheel(ev, c=widget):
                    try:
                        step = -2 if (ev.num == 4 or getattr(ev, 'delta', 0) > 0) else 2
                        c.yview_scroll(step, 'units')
                    except Exception:
                        pass
                widget.bind('<Enter>', lambda _e, c=widget: (c.bind_all('<MouseWheel>', _wheel), c.bind_all('<Button-4>', _wheel), c.bind_all('<Button-5>', _wheel)), add='+')
                widget.bind('<Leave>', lambda _e, c=widget: (c.unbind_all('<MouseWheel>'), c.unbind_all('<Button-4>'), c.unbind_all('<Button-5>')), add='+')
        except Exception:
            pass
        for child in widget.winfo_children():
            _walk(child)

    _walk(root_widget)


def _style_modern_radio_and_checkbox(elem):
    w = elem.Widget
    bg = BG_CARD
    try:
        bg = w.cget('background') or BG_CARD
    except Exception:
        pass
    fn = GLASS.render_radio if isinstance(elem, psg.Radio) else GLASS.render_checkbox
    sz = 20 if isinstance(elem, psg.Radio) else 19
    w._vec_imgs = (fn(False, False, bg, sz), fn(False, True, bg, sz), fn(True, False, bg, sz), fn(True, True, bg, sz))
    cur_txt = w.cget('text')
    if cur_txt and not cur_txt.startswith("  "):
        cur_txt = "  " + cur_txt.lstrip()
    w.configure(
        text=cur_txt, image=w._vec_imgs[0], selectimage=w._vec_imgs[2], indicatoron=False, compound='left',
        relief='flat', overrelief='flat', offrelief='flat', bd=0, highlightthickness=0,
        selectcolor=bg, activebackground=bg, activeforeground='#FFFFFF', cursor='hand2', padx=4, pady=3,
    )
    w.bind('<Enter>', lambda _e, wg=w: wg.configure(image=wg._vec_imgs[1], selectimage=wg._vec_imgs[3], foreground='#FFFFFF'), add='+')
    w.bind('<Leave>', lambda _e, wg=w: wg.configure(image=wg._vec_imgs[0], selectimage=wg._vec_imgs[2], foreground=TEXT_PRIMARY), add='+')


def finalize_sleek_window(win, glass_registry: dict, card_keys=None, reveal_alpha: bool = True):
    card_keys = card_keys or set()
    win._glass_registry = glass_registry
    if getattr(win, 'TKroot', None) is not None:
        _configure_modern_scrollbars_and_sliders(win.TKroot)
    for key, elem in win.key_dict.items():
        try:
            w = elem.Widget
            if isinstance(elem, psg.Button):
                w.configure(cursor="hand2", relief="flat", bd=0, highlightthickness=0, activebackground=w.cget("background"))
            elif isinstance(elem, (psg.Radio, psg.Checkbox)):
                _style_modern_radio_and_checkbox(elem)
            elif isinstance(elem, psg.Input):
                w.configure(relief="flat", bd=5, highlightthickness=1, highlightbackground=BORDER_INPUT, highlightcolor=ACCENT_CYAN)
            elif isinstance(elem, psg.Frame):
                w.configure(relief="flat", bd=1, highlightthickness=1, highlightbackground=BORDER_CARD, highlightcolor=BORDER_CARD)
            elif isinstance(elem, psg.Column):
                if key in card_keys:
                    w.configure(highlightthickness=1, highlightbackground=BORDER_CARD)
                if hasattr(w, 'canvas') and hasattr(w, 'TKFrame'):
                    w.TKFrame.update_idletasks()
                    w.canvas.configure(scrollregion=w.canvas.bbox("all"))
                    w.canvas.yview_moveto(0)
        except Exception:
            pass
    for btn_key in glass_registry:
        if btn_key in win.key_dict:
            win[btn_key].bind('<Enter>', '_GLASS_IN')
            win[btn_key].bind('<Leave>', '_GLASS_OUT')
    if getattr(win, 'TKroot', None) is not None:
        try:
            win.TKroot.update_idletasks()
        except Exception:
            pass
    if reveal_alpha:
        try:
            win.set_alpha(1.0)
        except Exception:
            pass


def handle_glass_hover_event(win, event) -> bool:
    if not isinstance(event, str):
        return False
    reg = getattr(win, "_glass_registry", {})
    if event.endswith('_GLASS_IN'):
        entry = reg.get(event[:-9])
        if entry and not entry["disabled"]:
            win[event[:-9]].update(image_data=entry["imgs"]["hover"])
        return True
    if event.endswith('_GLASS_OUT'):
        entry = reg.get(event[:-10])
        if entry:
            win[event[:-10]].update(image_data=entry["imgs"]["disabled" if entry["disabled"] else "normal"])
        return True
    return False


def set_glass_button_disabled(win, key: str, disabled: bool):
    entry = getattr(win, "_glass_registry", {}).get(key)
    if entry:
        entry["disabled"] = disabled
        win[key].update(disabled=disabled, image_data=entry["imgs"]["disabled" if disabled else "normal"])


# =====================================================================
# FEATURE SUITE REGISTRY, HEADERS & EXTRACTION ENGINE (36 SUITES, 359 COLS)
# =====================================================================
NIST_SUITE_DEFS = [
    ("1",  "longest_run_ones_in_a_block_test",       "1.1 Longest Run of Ones (Success)",             "1.2 Longest Run of Ones (p-value)",             longest_run_ones_in_a_block_test,       1),
    ("2",  "overlapping_template_matching_test",     "2.1 Overlapping Template (Success) [Adaptive]", "2.2 Overlapping Template (p-value) [Adaptive]", overlapping_template_matching_test,     1),
    ("3",  "approximate_entropy_test",               "3.1 Approximate Entropy (Success)",             "3.2 Approximate Entropy (p-value)",             approximate_entropy_test,               1),
    ("4",  "frequency_within_block_test",            "4.1 Frequency Within Block (Success)",          "4.2 Frequency Within Block (p-value)",          frequency_within_block_test,            1),
    ("5",  "runs_test",                              "5.1 Runs Test (Success)",                       "5.2 Runs Test (p-value)",                       runs_test,                              1),
    ("6",  "random_excursion_variant_test",          "6.1 Random Excursion Variant (Success)",        "6.2 Random Excursion Variant (18 p-values)",    random_excursion_variant_test,          18),
    ("7",  "maurers_universal_test",                 "7.1 Maurer's Universal (Success) [Adaptive]",   "7.2 Maurer's Universal (p-value) [Adaptive]",   maurers_universal_test,                 1),
    ("8",  "cumulative_sums_test",                   "8.1 Cumulative Sums (Success)",                 "8.2 Cumulative Sums (2 p-values)",              cumulative_sums_test,                   2),
    ("9",  "non_overlapping_template_matching_test", "9.1 Non-Overlapping Template (Success)",        "9.2 Non-Overlapping Template (p-value)",        non_overlapping_template_matching_test, 1),
    ("10", "monobit_test",                           "10.1 Monobit Frequency (Success)",              "10.2 Monobit Frequency (p-value)",              monobit_test,                           1),
    ("11", "random_excursion_test",                  "11.1 Random Excursion (Success)",               "11.2 Random Excursion (8 p-values)",            random_excursion_test,                  8),
    ("12", "dft_test",                               "12.1 Discrete Fourier Transform (Success)",     "12.2 Discrete Fourier Transform (p-value)",     dft_test,                               1),
    ("13", "serial_test",                            "13.1 Serial Test (Success)",                    "13.2 Serial Test (2 p-values)",                 serial_test,                            2),
    ("14", "binary_matrix_rank_test",                "14.1 Binary Matrix Rank (Success) [Adaptive]",  "14.2 Binary Matrix Rank (p-value) [Adaptive]",  binary_matrix_rank_test,                1),
    ("15", "linear_complexity_test",                 "15.1 Linear Complexity (Success) [Adaptive]",   "15.2 Linear Complexity (p-value) [Adaptive]",   linear_complexity_test,                 1),
]

HEDGE_UI_ITEMS = [
    ("16. Chi-Square Absolute Success",   "16_chi_test_absolute_success"),
    ("17. Chi-Square Confidence Success", "17_chi_test_confidence_success"),
    ("18.1 Chi-Square Uniform Statistic", "18_1_chi2_uniform_stat"),
    ("18.2 Chi-Square Uniform p-value",   "18_2_chi2_uniform_pvalue"),
]

BLFCP_UI_ITEMS = [
    ("19. Byte Frequency Distribution (256 cols)", "19_byte_frequency_distribution"),
    ("20. Discrete Fourier Transform (10 cols)",   "20_discrete_fourier_transform"),
    ("21. Lempel-Ziv Complexity (Normalized LZ76)","21_lempel_ziv_complexity"),
    ("22. Monte Carlo Pi Approximation",           "22_monte_carlo_pi_approximation"),
]

SOTA_SUITE_DEFS = [
    ("23_byte_moments",                 "23. Distributional Moments (Mean/Var/Skew/Kurt) [4]",   byte_statistical_moments,       ['23_byte_mean', '23_byte_variance', '23_byte_skewness', '23_byte_kurtosis'], True),
    ("24_ks_uniformity",                "24. Kolmogorov-Smirnov Uniformity (Stat & p-val) [2]",  ks_uniformity_test,             ['24_ks_uniform_stat', '24_ks_uniform_pvalue'], True),
    ("25_wasserstein_distance_uniform", "25. 1-Wasserstein (Earth Mover's) Distance to Uniform", wasserstein_uniform_distance,   ['25_wasserstein_distance_uniform'], False),
    ("26_index_of_coincidence",         "26. Index of Coincidence (IoC)",                        index_of_coincidence,           ['26_index_of_coincidence'], False),
    ("27_generalized_entropies",        "27. Rényi Collision, Min & Tsallis Entropies [3]",      generalized_entropies,          ['27_renyi_collision_entropy', '27_min_entropy', '27_tsallis_entropy'], True),
    ("28_serial_and_lag_autocorr",      "28. Serial Correlation & Lag-{2,4,8} Autocorr [4]",     serial_and_lag_autocorrelation, ['28_serial_corr_lag1', '28_autocorr_lag2', '28_autocorr_lag4', '28_autocorr_lag8'], True),
    ("29_markov_bigram_metrics",        "29. Markov Bigram & Mutual Info (ESORICS'26) [4]",      markov_bigram_features,         ['29_bigram_joint_entropy', '29_conditional_entropy', '29_mutual_information', '29_unique_bigram_ratio'], True),
    ("30_byte_difference_stats",        "30. Byte Difference Entropy & Mean Jump [2]",           byte_difference_features,       ['30_mod256_diff_entropy', '30_mean_abs_byte_jump'], True),
    ("31_recompression_ratios",         "31. Multi-Algo Re-Compression (zlib/bz2/lzma) [3]",     recompression_ratios,           ['31_recomp_ratio_zlib', '31_recomp_ratio_bz2', '31_recomp_ratio_lzma'], True),
    ("32_haar_wavelet_detail_ratio",    "32. 1-Level Haar Wavelet Detail Energy Ratio",          haar_wavelet_detail_ratio,      ['32_haar_wavelet_detail_ratio'], False),
    ("33_spectral_flatness_wiener",     "33. Spectral Flatness Measure (Wiener Entropy)",        spectral_flatness_measure,      ['33_spectral_flatness_wiener'], False),
    ("34_structural_run_and_ascii",     "34. Norm Max Byte Run & Printable ASCII Ratio [2]",     structural_run_and_ascii,       ['34_max_byte_run', '34_printable_ascii_ratio'], True),
]

AFRL_UI_ITEMS = [
    ("35.1 Local Nibbles Variance (Success)",      "35_1_local_nibbles_variance_success"),
    ("35.2 Local Nibbles Variance (Stat)",         "35_2_local_nibbles_variance_stat"),
    ("36. AFRL Combined Decision (Success)",       "36_afrl_combined_success"),
]

ALL_FEATURE_KEYS = (
    [k for num, base, _, _, _, _ in NIST_SUITE_DEFS for k in (f"{num}_1_{base}_success", f"{num}_2_{base}_pvalue")]
    + [k for _, k in HEDGE_UI_ITEMS]
    + [k for _, k in BLFCP_UI_ITEMS]
    + [k for k, _, _, _, _ in SOTA_SUITE_DEFS]
    + [k for _, k in AFRL_UI_ITEMS]
)
DISABLED_NIST_KEYS = set()

PRESET_AFRL = {
    '3_1_approximate_entropy_test_success',
    '4_1_frequency_within_block_test_success',
    '5_1_runs_test_success',
    '35_1_local_nibbles_variance_success',
    '35_2_local_nibbles_variance_stat',
    '36_afrl_combined_success',
}
PRESET_HEDGE = {
    '3_1_approximate_entropy_test_success', '4_1_frequency_within_block_test_success',
    '8_1_cumulative_sums_test_success', '16_chi_test_absolute_success', '17_chi_test_confidence_success',
}
PRESET_ENCOD = {'19_byte_frequency_distribution'}
PRESET_BLFCP = {
    '18_1_chi2_uniform_stat', '18_2_chi2_uniform_pvalue', '19_byte_frequency_distribution',
    '20_discrete_fourier_transform', '21_lempel_ziv_complexity', '22_monte_carlo_pi_approximation',
}
PRESET_SOTA = {k for k, _, _, _, _ in SOTA_SUITE_DEFS}


# =====================================================================
# DIALOGS & MODALS
# =====================================================================
def set_window_icon(win):
    try:
        icon_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "icons", "main_icon.png")
        if os.path.exists(icon_path):
            icon_photo = ImageTk.PhotoImage(Image.open(icon_path))
            win.TKroot.icon_photo = icon_photo
            win.TKroot.iconphoto(False, icon_photo)
    except Exception:
        pass


def show_guidance_modal():
    reg = {}
    guide_text = "\n".join([
        "WORKFLOW OVERVIEW (4-STAGE STUDIO PIPELINE)",
        "────────────────────────────────────────────────────────────",
        "1. Fragment Extractor:",
        "   Slice raw files from a dataset folder into fixed-size byte fragments (CSV).",
        "   • Supports ANY fragment length (e.g., 256B, 512B, 1024B, 2048B, 4096B, 8192B+).",
        "",
        "2. Feature Studio:",
        "   Extract 36 statistical, cryptographic, Markov, wavelet, and AFRL feature suites (359 columns)",
        "   for training ML/DL classifiers. All suites adapt automatically to any fragment length.",
        "",
        "3. Detection Methods (Direct Binary Classification: 1 = Encrypted, 0 = Unencrypted):",
        "   Run direct threshold/ensemble encryption detectors directly on fragment CSVs:",
        "   • AFRL Detector: Combines NIST Approximate Entropy, Block Frequency, Runs Test,",
        "     and Local Nibbles Variance (LNV <= 46.98 on 64-byte sliding windows).",
        "   • HEDGE Detector: Combines NIST Approximate Entropy, Block Frequency, Cumulative Sums,",
        "     Chi-Square Absolute Band (gamma=2), and Chi-Square Confidence (0.01 <= p <= 0.99).",
        "   • SOTA '26 Detector: 7-stage statistical hypothesis detector (IoC, KS + Wasserstein-1,",
        "     Lag-{1,2,4,8} Autocorr, Markov Bigram Ratio, Wiener SFM + Haar Wavelet, Recompression, ASCII).",
        "   • Output CSV contains 1 (Encrypted) or 0 (Unencrypted) per fragment row.",
        "",
        "4. Combine Datasets:",
        "   Merge CSV datasets vertically along rows (Axis 0) or side-by-side along columns (Axis 1).",
        "",
        "FEATURE STUDIO BENCHMARK PRESETS",
        "────────────────────────────────────────────────────────────",
        "• AFRL Suite: 3_1, 4_1, 5_1, 35_1_local_nibbles_variance_success, 35_2_stat, 36_afrl_combined_success",
        "• HEDGE Full Suite: 3_1, 4_1, 8_1, 16_chi_test_absolute_success, 17_chi_test_confidence_success",
        "• EnCoD Suite: 19_byte_frequency_distribution (256 normalized byte frequencies)",
        "• BLFCP Full Suite: 18_1, 18_2, 19 (256), 20 (10 DFT), 21 (Normalized LZ76), 22 (Monte Carlo Pi)",
        "• SOTA 2024–2026 Suite (28 cols): Moments, KS, Wasserstein, IoC, Rényi/Min/Tsallis Entropies,",
        "  Lag-{1,2,4,8} Autocorr, Markov Bigrams/MI, Byte Diffs, Re-Compression, Haar Wavelet, SFM, ASCII",
    ])
    layout = [
        [psg.Text("User Guide & Benchmark Reference", font=FONT_TITLE, text_color=ACCENT_CYAN)],
        [psg.Column([[psg.Multiline(guide_text, size=(78, 22), font=FONT_MONO, disabled=True, background_color=BG_CARD, text_color=TEXT_PRIMARY, border_width=0, pad=(8, 8))]],
                    key="-CARD-GUIDE-", background_color=BG_CARD, pad=(4, 6))],
        [psg.Push(), make_glass_btn(reg, "-CLOSE-", "Got It", 140, 38, palette="emerald", parent_bg=BG_MAIN, icon="check"), psg.Push()]
    ]
    win = psg.Window("Guidance & Documentation", layout, modal=True, alpha_channel=0, finalize=True)
    set_window_icon(win)
    finalize_sleek_window(win, reg, {"-CARD-GUIDE-"})
    while True:
        ev, _ = win.read()
        if ev in ("Exit", psg.WIN_CLOSED, "-CLOSE-"):
            break
        handle_glass_hover_event(win, ev)
    win.close()


def show_about_modal():
    reg = {}
    desc_text = "\n".join([
        "Universal any-length fragment slicer, statistical feature extraction suite,",
        "and direct encrypted fragment detection studio (NIST SP 800-22, AFRL, HEDGE, BLFCP, EnCoD, SOTA 2026)."
    ])
    layout = [
        [psg.Column([
            [psg.Text("AFRL", font=FONT_TITLE, text_color=ACCENT_CYAN, background_color=BG_CARD)],
            [psg.Text("An Open-Source Software Tool for Distinguishing Encrypted File Fragments from High-Entropy Data", font=FONT_BOLD, text_color=TEXT_PRIMARY, background_color=BG_CARD)],
            [psg.Text("Version 1.1.0 — Studio Edition", font=FONT_BOLD, text_color=TEXT_MUTED, background_color=BG_CARD)],
            [psg.HorizontalSeparator(color=BORDER_CARD)],
            [psg.Text(desc_text, font=('Segoe UI', 10), text_color=TEXT_PRIMARY, background_color=BG_CARD)],
            [psg.Text("Created by: Alireza Aliaskari Hosseinabadi", font=FONT_BOLD, background_color=BG_CARD, pad=((0, 0), (10, 4)))],
            [psg.Text("License: GNU General Public License v3.0 (GPL-3.0)", font=FONT_SUBTITLE, text_color=TEXT_MUTED, background_color=BG_CARD)],
            [psg.VPush(background_color=BG_CARD)],
            [psg.Push(background_color=BG_CARD), make_glass_btn(reg, "-OK-", "Close", 140, 38, palette="emerald", parent_bg=BG_CARD, icon="check"), psg.Push(background_color=BG_CARD)]
        ], key="-CARD-ABOUT-", background_color=BG_CARD, pad=(18, 18), expand_x=True, expand_y=True)]
    ]
    win = psg.Window("About Application", layout, modal=True, alpha_channel=0, finalize=True)
    set_window_icon(win)
    finalize_sleek_window(win, reg, {"-CARD-ABOUT-"})
    while True:
        ev, _ = win.read()
        if ev in ("Exit", psg.WIN_CLOSED, "-OK-"):
            break
        handle_glass_hover_event(win, ev)
    win.close()


def create_progress_modal(title, subtitle):
    reg = {}
    layout = [
        [psg.Column([
            [psg.Text(title, font=FONT_SECTION, text_color=ACCENT_CYAN, background_color=BG_CARD)],
            [psg.Text(subtitle, font=FONT_SUBTITLE, text_color=TEXT_MUTED, background_color=BG_CARD)],
            [psg.HorizontalSeparator(color=BORDER_CARD)],
            [psg.Text("Status:", font=FONT_BOLD, background_color=BG_CARD, text_color=TEXT_MUTED),
             psg.Text("Ready to start", key='-STATUS-', font=FONT_BOLD, background_color=BG_CARD, text_color=TEXT_PRIMARY, size=(42, 1))],
            [psg.ProgressBar(100, orientation='h', size=(48, 22), key='-PROGRESS-')],
            [psg.Text("Elapsed: 00:00:00  |  ETR: --:--:--", key='-TIME-', font=FONT_MONO, background_color=BG_CARD, text_color=ACCENT_AMBER)],
            [psg.Push(background_color=BG_CARD),
             make_glass_btn(reg, "Start", "Start Execution", 170, 38, palette="emerald", parent_bg=BG_CARD, icon="play", pad=(6, 10)),
             make_glass_btn(reg, "-CNL-", "Cancel", 130, 38, palette="ruby", parent_bg=BG_CARD, icon="close", disabled=True, pad=(6, 10)),
             psg.Push(background_color=BG_CARD)]
        ], key="-CARD-PROG-", background_color=BG_CARD, pad=(18, 18), expand_x=True)]
    ]
    win = psg.Window(title, layout, modal=True, alpha_channel=0, finalize=True)
    set_window_icon(win)
    finalize_sleek_window(win, reg, {"-CARD-PROG-"})
    return win


# =====================================================================
# CORE HELPER & WORKER FUNCTIONS (FEATURE EXTRACTION + DETECTION)
# =====================================================================
def format_time(seconds):
    if seconds is None or seconds < 0:
        return "Calculating..."
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def process_row(row, dic_checklist):
    row_np = np.asarray(row, dtype=np.uint8)
    frag_len = row_np.size
    all_features = []

    # NIST SP 800-22 (1..15)
    bits = None
    for num, base, _, _, fn, p_cnt in NIST_SUITE_DEFS:
        want_s = dic_checklist.get(f"{num}_1_{base}_success", False)
        want_p = dic_checklist.get(f"{num}_2_{base}_pvalue", False)
        if want_s or want_p:
            if bits is None:
                bits = np.unpackbits(row_np)
            success, pval, extra = fn(bits)
            if want_s:
                all_features.append(1 if success else 0)
            if want_p:
                if p_cnt > 1:
                    all_features.extend(extra)
                else:
                    all_features.append(pval)

    # HEDGE Chi-Square (16..18)
    if any(dic_checklist.get(k, False) for _, k in HEDGE_UI_ITEMS):
        chi2_stat, p_value = chi2_uniform(row_np, Length=frag_len)
        if dic_checklist.get("16_chi_test_absolute_success"):
            all_features.append(chi_test_absolute(chi2_stat, length=frag_len))
        if dic_checklist.get("17_chi_test_confidence_success"):
            all_features.append(chi_test_confidence(p_value))
        if dic_checklist.get("18_1_chi2_uniform_stat"):
            all_features.append(chi2_stat)
        if dic_checklist.get("18_2_chi2_uniform_pvalue"):
            all_features.append(p_value)

    # BLFCP & EnCoD (19..22)
    if dic_checklist.get("19_byte_frequency_distribution"):
        all_features.extend(byte_frequency_distribution(row_np))
    if dic_checklist.get("20_discrete_fourier_transform"):
        all_features.extend(discrete_fourier_transform(row_np))
    if dic_checklist.get("21_lempel_ziv_complexity"):
        all_features.append(lempel_ziv_complexity(row_np))
    if dic_checklist.get("22_monte_carlo_pi_approximation"):
        all_features.append(monte_carlo_pi_approximation(row_np))

    # SOTA 2024-2026 Multi-Modal, Markov & Spectral (23..34)
    for key, _, fn, _, is_list in SOTA_SUITE_DEFS:
        if dic_checklist.get(key):
            res = fn(row_np)
            if is_list:
                all_features.extend(res)
            else:
                all_features.append(res)

    # AFRL Features (35..36)
    if dic_checklist.get("35_1_local_nibbles_variance_success") or dic_checklist.get("35_2_local_nibbles_variance_stat"):
        lnv_success, lnv_stat, _ = local_nibbles_variance(row_np)
        if dic_checklist.get("35_1_local_nibbles_variance_success"):
            all_features.append(1 if lnv_success else 0)
        if dic_checklist.get("35_2_local_nibbles_variance_stat"):
            all_features.append(lnv_stat)
    if dic_checklist.get("36_afrl_combined_success"):
        all_features.append(int(AFRL_test(row_np)))

    return all_features


def generate_feature_headers(dic_checklist):
    headers = []
    for num, base, _, _, _, p_cnt in NIST_SUITE_DEFS:
        if dic_checklist.get(f"{num}_1_{base}_success"):
            headers.append(f"{num}_1_{base}_success")
        if dic_checklist.get(f"{num}_2_{base}_pvalue"):
            if p_cnt > 1:
                headers.extend(f"{num}_2_{base}_pvalue_{i}" for i in range(p_cnt))
            else:
                headers.append(f"{num}_2_{base}_pvalue")

    for _, k in HEDGE_UI_ITEMS:
        if dic_checklist.get(k):
            headers.append(k)

    if dic_checklist.get("19_byte_frequency_distribution"):
        headers.extend(f"19_byte_frequency_distribution_{i}" for i in range(256))
    if dic_checklist.get("20_discrete_fourier_transform"):
        headers.extend(f"20_discrete_fourier_transform_{i}" for i in range(10))
    if dic_checklist.get("21_lempel_ziv_complexity"):
        headers.append("21_lempel_ziv_complexity")
    if dic_checklist.get("22_monte_carlo_pi_approximation"):
        headers.append("22_monte_carlo_pi_approximation")

    for key, _, _, cols, _ in SOTA_SUITE_DEFS:
        if dic_checklist.get(key):
            headers.extend(cols)

    for _, k in AFRL_UI_ITEMS:
        if dic_checklist.get(k):
            headers.append(k)

    return headers


def _fast_count_lines(file_path: str) -> int:
    """Fast binary buffered newline counter for large CSV files."""
    count = 0
    with open(file_path, 'rb') as f:
        while True:
            buf = f.read(1048576)
            if not buf:
                break
            count += buf.count(b'\n')
    return max(1, count)


def parallel_feature_extraction_large_file(window, input_file_path, output_file_path, dic_checklist, cancel_event, chunk_size=200):
    try:
        num_cores = max(1, int(mp.cpu_count() * dic_checklist["-cpu-"] / 100))
        window.write_event_value('-THREAD-UPDATE-', (0, 100, 'Stage 1/2: Counting fragments...', 'Calculating...'))
        row_count = _fast_count_lines(input_file_path)
        total_chunks = max(1, ceil(row_count / chunk_size))
        if cancel_event.is_set():
            window.write_event_value('-THREAD-DONE-', (False, "Cancelled by user."))
            return

        window.write_event_value('-THREAD-UPDATE-', (0, total_chunks, f'Stage 2/2: Processing across {num_cores} CPU cores...', 'Starting...'))
        start_time = time.time()
        is_first_chunk = True
        header_list = generate_feature_headers(dic_checklist)
        worker_fn = partial(process_row, dic_checklist=dic_checklist)
        mp_subchunk = max(1, chunk_size // (num_cores * 2))

        with mp.Pool(processes=num_cores) as pool:
            for i, chunk_df in enumerate(pd.read_csv(input_file_path, header=None, dtype=np.uint8, chunksize=chunk_size, engine='c')):
                if cancel_event.is_set():
                    window.write_event_value('-THREAD-DONE-', (False, "Process cancelled by user."))
                    return
                results = pool.map(worker_fn, chunk_df.to_numpy(dtype=np.uint8), chunksize=mp_subchunk)
                pd.DataFrame(results).to_csv(
                    output_file_path, index=False,
                    mode='w' if is_first_chunk else 'a',
                    header=header_list if is_first_chunk else False,
                )
                is_first_chunk = False
                cw = i + 1
                elapsed = time.time() - start_time
                pct = min(1.0, cw / total_chunks)
                etr = (elapsed / pct) - elapsed if pct > 0 else 0
                window.write_event_value('-THREAD-UPDATE-', (cw, total_chunks, f'Processed chunk {cw} of {total_chunks} ({int(pct * 100)}%)', f"Elapsed: {format_time(elapsed)} | ETR: {format_time(etr)}"))

        window.write_event_value('-THREAD-DONE-', (True, f"Features successfully saved to: {output_file_path}"))
    except Exception as e:
        window.write_event_value('-THREAD-ERROR-', str(e))


# =====================================================================
# DETECTION METHODS WORKER (1 = ENCRYPTED, 0 = UNENCRYPTED)
# =====================================================================
DETECTION_METHODS_REGISTRY = {
    "AFRL": {
        "name": "AFRL Ensemble Detector (AppEn + BlockFreq + Runs + LNV)",
        "func": AFRL_test,
        "header": "AFRL_encrypted",
        "description": (
            "AFRL Method: Evaluates each byte fragment across 4 vectorized criteria:\n"
            "  1) NIST Approximate Entropy Test (p >= 0.01)\n"
            "  2) NIST Frequency Within Block Test (p >= 0.01)\n"
            "  3) NIST Runs Test (p >= 0.01)\n"
            "  4) Local Nibbles Variance (LNV <= 46.98 across 64-byte sliding windows)\n"
            "Output: 1 = Encrypted fragment (passes all 4 checks), 0 = Unencrypted fragment."
        ),
    },
    "HEDGE": {
        "name": "HEDGE Ensemble Detector (AppEn + BlockFreq + Cusum + Chi2 Abs & Conf)",
        "func": HEDGE_test,
        "header": "HEDGE_encrypted",
        "description": (
            "HEDGE Method: Evaluates each byte fragment across 5 statistical uniformity criteria:\n"
            "  1) NIST Approximate Entropy Test (p >= 0.01)\n"
            "  2) NIST Frequency Within Block Test (p >= 0.01)\n"
            "  3) NIST Cumulative Sums (Forward & Backward) Test (p >= 0.01)\n"
            "  4) Chi-Square Uniformity Absolute Band (mu +/- 2*sigma, adaptive to fragment length L)\n"
            "  5) Chi-Square Uniformity Confidence Interval (0.01 <= p-value <= 0.99)\n"
            "Output: 1 = Encrypted fragment (passes all 5 checks), 0 = Unencrypted fragment."
        ),
    },
    "SOTA": {
        "name": "SOTA '26 Multi-Modal Statistical Detector (IoC + Wasserstein + Markov + Spectral + Recomp)",
        "func": SOTA_test,
        "header": "SOTA_encrypted",
        "description": (
            "SOTA '26 Multi-Modal Detector: Evaluates 7 orthogonal scale-adaptive criteria under H0:\n"
            "  1) Index of Coincidence (IoC) & KS + 1-Wasserstein Uniformity Distance\n"
            "  2) Multi-Lag Autocorrelation (Lags 1, 2, 4, 8) & Markov 1st-Order Bigram Independence\n"
            "  3) Spectral Flatness (Wiener Entropy) & 1-Level Haar Wavelet Detail Ratio\n"
            "  4) Multi-Algo Kolmogorov Incompressibility (zlib/bz2/lzma >= 0.995) & Anti-Evasion ASCII/Min-Entropy\n"
            "Output: 1 = Encrypted fragment (passes all 7 criteria), 0 = Unencrypted fragment."
        ),
    },
}


def detect_row_dispatch(row, method_key: str) -> int:
    return int(DETECTION_METHODS_REGISTRY[method_key]["func"](row))


def parallel_detection_large_file(window, input_file_path, output_file_path, method_key, cpu_percent, include_header, cancel_event, chunk_size=400):
    try:
        num_cores = max(1, int(mp.cpu_count() * cpu_percent / 100))
        window.write_event_value('-THREAD-UPDATE-', (0, 100, 'Stage 1/2: Counting fragments...', 'Calculating...'))
        row_count = _fast_count_lines(input_file_path)
        total_chunks = max(1, ceil(row_count / chunk_size))
        if cancel_event.is_set():
            window.write_event_value('-THREAD-DONE-', (False, "Cancelled by user."))
            return

        window.write_event_value('-THREAD-UPDATE-', (0, total_chunks, f'Stage 2/2: Running {method_key} across {num_cores} CPU cores...', 'Starting...'))
        start_time = time.time()
        is_first_chunk, total_processed, encrypted_count = True, 0, 0
        col_header = [DETECTION_METHODS_REGISTRY[method_key]["header"]]
        worker_fn = partial(detect_row_dispatch, method_key=method_key)
        mp_subchunk = max(1, chunk_size // (num_cores * 2))

        with mp.Pool(processes=num_cores) as pool:
            for i, chunk_df in enumerate(pd.read_csv(input_file_path, header=None, dtype=np.uint8, chunksize=chunk_size, engine='c')):
                if cancel_event.is_set():
                    window.write_event_value('-THREAD-DONE-', (False, "Detection stopped by user."))
                    return
                preds_arr = np.asarray(pool.map(worker_fn, chunk_df.to_numpy(dtype=np.uint8), chunksize=mp_subchunk), dtype=np.uint8)
                total_processed += preds_arr.size
                encrypted_count += int(np.sum(preds_arr))
                pd.DataFrame(preds_arr).to_csv(
                    output_file_path, index=False,
                    mode='w' if is_first_chunk else 'a',
                    header=col_header if (is_first_chunk and include_header) else False,
                )
                is_first_chunk = False
                cw = i + 1
                elapsed = time.time() - start_time
                pct = min(1.0, cw / total_chunks)
                etr = (elapsed / pct) - elapsed if pct > 0 else 0
                window.write_event_value('-THREAD-UPDATE-', (cw, total_chunks, f'Classified chunk {cw} of {total_chunks} ({int(pct * 100)}%)', f"Elapsed: {format_time(elapsed)} | ETR: {format_time(etr)}"))

        unenc = total_processed - encrypted_count
        enc_pct = (100.0 * encrypted_count / total_processed) if total_processed else 0.0
        unenc_pct = (100.0 * unenc / total_processed) if total_processed else 0.0
        summary = (
            f"Method: {method_key}\n"
            f"Total Fragments Evaluated: {total_processed}\n"
            f"  • Encrypted (1):   {encrypted_count} ({enc_pct:.2f}%)\n"
            f"  • Unencrypted (0): {unenc} ({unenc_pct:.2f}%)\n\n"
            f"Saved predictions to:\n{output_file_path}"
        )
        window.write_event_value('-THREAD-DONE-', (True, summary))
    except Exception as e:
        window.write_event_value('-THREAD-ERROR-', str(e))


# =====================================================================
# ACTION WORKFLOWS
# =====================================================================
def run_fragment_extraction_workflow(mode, src_folder, frag_size_str, count_str, dst_file):
    if not src_folder or not os.path.isdir(src_folder):
        psg.popup_error("Please select a valid source folder.", title="Validation Error")
        return
    if not dst_file:
        psg.popup_error("Please specify a destination CSV file.", title="Validation Error")
        return
    try:
        frag_size, frag_count = int(frag_size_str), int(count_str)
        if frag_size <= 0 or frag_count <= 0:
            raise ValueError
    except ValueError:
        psg.popup_error("Fragment size and count must be positive integers.", title="Validation Error")
        return

    titles = {
        "FOLDER_RANDOM": ("Dataset Random Fragment Extractor", f"Extracting {frag_count} total fragments ({frag_size} bytes each)", extract_whole_folder_fragments),
        "FILE_RANDOM":   ("Per-File Fragment Extractor", f"Extracting {frag_count} fragments per file ({frag_size} bytes each)", extract_random_fragments_file),
        "FILE_PADDED":   ("Padded Per-File Fragment Extractor", f"Extracting {frag_count} fragments per file with random padding ({frag_size} bytes)", extract_random_fragments_file_random_pad),
    }
    title, subtitle, fn = titles[mode]
    win_prog = create_progress_modal(title, subtitle)
    while True:
        ev, _ = win_prog.read()
        if ev in ("Exit", psg.WIN_CLOSED):
            break
        if handle_glass_hover_event(win_prog, ev):
            continue
        if ev == "Start":
            set_glass_button_disabled(win_prog, "-CNL-", False)
            set_glass_button_disabled(win_prog, "Start", True)
            if fn(win_prog, src_folder, frag_size, frag_count, dst_file):
                psg.popup("Extraction Complete!", f"Fragments saved to: {dst_file}", title="Success")
            else:
                psg.popup("Cancelled", "Fragment extraction was stopped.", title="Cancelled")
            break
    win_prog.close()


def open_feature_extractor_studio(input_csv_path="", output_csv_path=""):
    reg = {}
    nist_rows = []
    for num, base, lbl_s, lbl_p, _, _ in NIST_SUITE_DEFS:
        nist_rows.append([psg.Checkbox(lbl_s, key=f"{num}_1_{base}_success", background_color=BG_CARD)])
        nist_rows.append([psg.Checkbox(lbl_p, key=f"{num}_2_{base}_pvalue", background_color=BG_CARD)])

    hedge_rows = [[psg.Checkbox(lbl, key=k, background_color=BG_CARD)] for lbl, k in HEDGE_UI_ITEMS]
    blfcp_rows = [[psg.Checkbox(lbl, key=k, background_color=BG_CARD)] for lbl, k in BLFCP_UI_ITEMS]
    sota_rows  = [[psg.Checkbox(lbl, key=k, background_color=BG_CARD)] for k, lbl, _, _, _ in SOTA_SUITE_DEFS]
    afrl_rows  = [[psg.Checkbox(lbl, key=k, background_color=BG_CARD)] for lbl, k in AFRL_UI_ITEMS]

    nist_frame = psg.Frame(' NIST SP 800-22 Suite (All 15 Adaptive) ', [[psg.Column(nist_rows, scrollable=True, vertical_scroll_only=True, size=(365, 345), background_color=BG_CARD)]],
                           background_color=BG_CARD, title_color=ACCENT_CYAN, font=FONT_SECTION, pad=(5, 5), relief=psg.RELIEF_FLAT)
    hedge_frame = psg.Frame(' HEDGE Chi-Square Suite ', [[psg.Column(hedge_rows, background_color=BG_CARD)]],
                            background_color=BG_CARD, title_color=ACCENT_CYAN, font=FONT_SECTION, pad=(5, 5), expand_x=True, relief=psg.RELIEF_FLAT)
    blfcp_frame = psg.Frame(' BLFCP & EnCoD Suite ', [[psg.Column(blfcp_rows, background_color=BG_CARD)]],
                            background_color=BG_CARD, title_color=ACCENT_CYAN, font=FONT_SECTION, pad=(5, 5), expand_x=True, relief=psg.RELIEF_FLAT)
    sota_frame = psg.Frame(' SOTA Multi-Modal & Markov Suite (2024–2026) ', [[psg.Column(sota_rows, scrollable=True, vertical_scroll_only=True, size=(400, 210), background_color=BG_CARD)]],
                           background_color=BG_CARD, title_color=ACCENT_CYAN, font=FONT_SECTION, pad=(5, 5), expand_x=True, relief=psg.RELIEF_FLAT)
    afrl_frame = psg.Frame(' AFRL Method Features (LNV & Ensemble) ', [[psg.Column(afrl_rows, background_color=BG_CARD)]],
                           background_color=BG_CARD, title_color=ACCENT_CYAN, font=FONT_SECTION, pad=(5, 5), expand_x=True, relief=psg.RELIEF_FLAT)

    preset_bar = psg.Frame(' Quick Selection & Benchmark Presets ', [[
        make_glass_btn(reg, "-PRESET-ALL-",   "Select All", 118, 34, palette="smoked", parent_bg=BG_CARD, radius=9, font_size=11, icon="check", pad=(4, 5)),
        make_glass_btn(reg, "-PRESET-NONE-",  "Clear All",  110, 34, palette="smoked", parent_bg=BG_CARD, radius=9, font_size=11, icon="close", pad=(4, 5)),
        psg.Text("│", text_color=BORDER_CARD, background_color=BG_CARD, font=FONT_TITLE),
        make_glass_btn(reg, "-PRESET-AFRL-",  "AFRL",        96, 34, palette="indigo", parent_bg=BG_CARD, radius=9, font_size=11, icon="star",  pad=(4, 5)),
        make_glass_btn(reg, "-PRESET-HEDGE-", "HEDGE",      104, 34, palette="indigo", parent_bg=BG_CARD, radius=9, font_size=11, icon="star",  pad=(4, 5)),
        make_glass_btn(reg, "-PRESET-BLFCP-", "BLFCP",      102, 34, palette="indigo", parent_bg=BG_CARD, radius=9, font_size=11, icon="star",  pad=(4, 5)),
        make_glass_btn(reg, "-PRESET-ENCOD-", "EnCoD",      104, 34, palette="indigo", parent_bg=BG_CARD, radius=9, font_size=11, icon="star",  pad=(4, 5)),
        make_glass_btn(reg, "-PRESET-SOTA-",  "SOTA '26",   114, 34, palette="indigo", parent_bg=BG_CARD, radius=9, font_size=11, icon="star",  pad=(4, 5)),
        make_glass_btn(reg, "-PRESET-NIST-",  "All NIST",   112, 34, palette="cyan",   parent_bg=BG_CARD, radius=9, font_size=11, icon="bolt",  pad=(4, 5)),
    ]], background_color=BG_CARD, title_color=TEXT_MUTED, font=FONT_SUBTITLE, expand_x=True, pad=(5, 5), relief=psg.RELIEF_FLAT)

    io_bar = psg.Frame(' Dataset Input / Output & Parallel Execution Settings ', [
        [psg.Text("Fragments CSV:", size=(16, 1), background_color=BG_CARD, font=FONT_BOLD),
         psg.Input(default_text=input_csv_path, key="-FEAT-IN-", readonly=True, disabled_readonly_background_color=BG_INPUT, disabled_readonly_text_color=TEXT_PRIMARY, expand_x=True),
         make_glass_btn(reg, "-BRW-FEAT-IN-", "Browse CSV", 142, 34, palette="smoked", parent_bg=BG_CARD, radius=9, font_size=11, icon="folder", pad=(6, 3), btn_kind="file")],
        [psg.Text("Output Features:", size=(16, 1), background_color=BG_CARD, font=FONT_BOLD),
         psg.Input(default_text=output_csv_path, key="-FEAT-OUT-", readonly=True, disabled_readonly_background_color=BG_INPUT, disabled_readonly_text_color=TEXT_PRIMARY, expand_x=True),
         make_glass_btn(reg, "-SAV-FEAT-OUT-", "Save As CSV", 142, 34, palette="smoked", parent_bg=BG_CARD, radius=9, font_size=11, icon="save", pad=(6, 3), btn_kind="save")],
        make_cpu_slider_row("-cpu-", "-FEAT-CPU-BADGE-", default_val=80, bg_hex=BG_CARD),
    ], background_color=BG_CARD, title_color=ACCENT_CYAN, font=FONT_SECTION, expand_x=True, pad=(5, 5), relief=psg.RELIEF_FLAT)

    layout = [
        [psg.Text("Statistical & Cryptographic Feature Extraction Studio", font=FONT_TITLE, text_color=ACCENT_CYAN), psg.Push(), psg.Text("● Universal Any-Length Engine", font=FONT_BOLD, text_color=ACCENT_EMERALD)],
        [preset_bar],
        [nist_frame, psg.Column([[hedge_frame], [blfcp_frame]], pad=(0, 0), expand_y=True), psg.Column([[sota_frame], [afrl_frame]], pad=(0, 0), expand_y=True)],
        [io_bar],
        [psg.Push(),
         make_glass_btn(reg, "-CANCEL-STUDIO-", "Cancel", 140, 40, palette="smoked", parent_bg=BG_MAIN, radius=10, font_size=12, icon="close", pad=(8, 6)),
         make_glass_btn(reg, "-RUN-STUDIO-", "Start Feature Extraction", 260, 40, palette="emerald", parent_bg=BG_MAIN, radius=10, font_size=12, icon="play", pad=(8, 6)),
         psg.Push()]
    ]

    win_feat = psg.Window("Feature Extraction Studio", layout, resizable=True, modal=True, alpha_channel=0, finalize=True)
    set_window_icon(win_feat)
    cpu_state = {"-cpu-": 80}
    cpu_slider = SleekCpuSlider(win_feat["-cpu-"], win_feat["-FEAT-CPU-BADGE-"], cpu_state, "-cpu-", default_val=80, bg_hex=BG_CARD)
    finalize_sleek_window(win_feat, reg)
    cpu_slider.redraw()

    preset_map = {
        "-PRESET-ALL-":   set(ALL_FEATURE_KEYS) - DISABLED_NIST_KEYS,
        "-PRESET-NONE-":  set(),
        "-PRESET-AFRL-":  PRESET_AFRL,
        "-PRESET-HEDGE-": PRESET_HEDGE,
        "-PRESET-BLFCP-": PRESET_BLFCP,
        "-PRESET-ENCOD-": PRESET_ENCOD,
        "-PRESET-SOTA-":  PRESET_SOTA,
        "-PRESET-NIST-":  {k for k in ALL_FEATURE_KEYS if k.split('_')[0].isdigit() and int(k.split('_')[0]) <= 15} - DISABLED_NIST_KEYS,
    }

    while True:
        event, values = win_feat.read()
        if event in ("Exit", psg.WIN_CLOSED, "-CANCEL-STUDIO-"):
            win_feat.close()
            return
        if handle_glass_hover_event(win_feat, event):
            continue
        if event in preset_map:
            sel = preset_map[event]
            for k in ALL_FEATURE_KEYS:
                if k not in DISABLED_NIST_KEYS:
                    win_feat[k].update(value=(k in sel))
        elif event == "-RUN-STUDIO-":
            in_file = values.get("-FEAT-IN-", "").strip()
            out_file = values.get("-FEAT-OUT-", "").strip()
            if not in_file or not os.path.isfile(in_file):
                psg.popup_error("Please select a valid input fragments CSV file.", title="Input Required")
                continue
            if not out_file:
                psg.popup_error("Please specify an output CSV file path.", title="Output Required")
                continue
            if not any(values.get(k, False) for k in ALL_FEATURE_KEYS):
                psg.popup_error("Please select at least one feature or preset to extract.", title="No Features Selected")
                continue
            dic_checklist = dict(values)
            dic_checklist["-cpu-"] = cpu_state["-cpu-"]
            win_feat.close()
            break

    win_prog = create_progress_modal("Parallel Feature Extraction", f"Extracting {len(generate_feature_headers(dic_checklist))} feature columns from dataset...")
    worker_thread, cancel_event = None, threading.Event()
    while True:
        event, prog_values = win_prog.read()
        if event in ("Exit", psg.WIN_CLOSED):
            if worker_thread and worker_thread.is_alive():
                cancel_event.set()
            break
        if handle_glass_hover_event(win_prog, event):
            continue
        if event == 'Start':
            set_glass_button_disabled(win_prog, "-CNL-", False)
            set_glass_button_disabled(win_prog, "Start", True)
            cancel_event.clear()
            worker_thread = threading.Thread(target=parallel_feature_extraction_large_file, args=(win_prog, in_file, out_file, dic_checklist, cancel_event), daemon=True)
            worker_thread.start()
        elif event == '-CNL-':
            win_prog['-STATUS-'].update("Stopping workers...")
            cancel_event.set()
        elif event == '-THREAD-UPDATE-':
            c, m, st, ts = prog_values[event]
            win_prog['-PROGRESS-'].update(c, max=m)
            win_prog['-STATUS-'].update(st)
            win_prog['-TIME-'].update(ts)
        elif event == '-THREAD-ERROR-':
            psg.popup_error(f"An error occurred during feature extraction: {prog_values[event]}", title="Worker Error")
            break
        elif event == '-THREAD-DONE-':
            ok, msg = prog_values[event]
            psg.popup("Extraction Complete!" if ok else "Cancelled", msg, title="Success" if ok else "Stopped")
            break
    win_prog.close()


def run_detection_method_workflow(method_key, in_csv, out_csv, cpu_percent, include_header):
    if not in_csv or not os.path.isfile(in_csv):
        psg.popup_error("Please select a valid input fragments CSV file.", title="Validation Error")
        return
    if not out_csv:
        psg.popup_error("Please specify an output predictions CSV file.", title="Validation Error")
        return

    win_prog = create_progress_modal(
        f"Encrypted Fragment Detector ({method_key})",
        f"Running {DETECTION_METHODS_REGISTRY[method_key]['name']} (1 = Encrypted, 0 = Unencrypted)..."
    )
    worker_thread, cancel_event = None, threading.Event()
    while True:
        event, prog_values = win_prog.read()
        if event in ("Exit", psg.WIN_CLOSED):
            if worker_thread and worker_thread.is_alive():
                cancel_event.set()
            break
        if handle_glass_hover_event(win_prog, event):
            continue
        if event == 'Start':
            set_glass_button_disabled(win_prog, "-CNL-", False)
            set_glass_button_disabled(win_prog, "Start", True)
            cancel_event.clear()
            worker_thread = threading.Thread(target=parallel_detection_large_file, args=(win_prog, in_csv, out_csv, method_key, cpu_percent, include_header, cancel_event), daemon=True)
            worker_thread.start()
        elif event == '-CNL-':
            win_prog['-STATUS-'].update("Stopping detection workers...")
            cancel_event.set()
        elif event == '-THREAD-UPDATE-':
            c, m, st, ts = prog_values[event]
            win_prog['-PROGRESS-'].update(c, max=m)
            win_prog['-STATUS-'].update(st)
            win_prog['-TIME-'].update(ts)
        elif event == '-THREAD-ERROR-':
            psg.popup_error(f"An error occurred during detection:\n\n{prog_values[event]}", title="Detection Error")
            break
        elif event == '-THREAD-DONE-':
            ok, msg = prog_values[event]
            psg.popup("Detection Complete!" if ok else "Cancelled", msg, title="Classification Summary" if ok else "Stopped")
            break
    win_prog.close()


def run_csv_combine_workflow(axis_mode, csv1, csv2, has_header, dst_csv):
    if not csv1 or not os.path.isfile(csv1) or not csv2 or not os.path.isfile(csv2):
        psg.popup_error("Please select two valid existing CSV files to combine.", title="Validation Error")
        return
    if not dst_csv:
        psg.popup_error("Please select a destination file path for the merged CSV.", title="Validation Error")
        return

    title = "Combine CSVs Along Rows (Axis 0)" if axis_mode == "ROWS" else "Combine CSVs Along Columns (Axis 1)"
    subtitle = "Appending rows vertically..." if axis_mode == "ROWS" else "Merging feature columns side-by-side..."
    win_prog = create_progress_modal(title, subtitle)
    while True:
        ev, _ = win_prog.read()
        if ev in ("Exit", psg.WIN_CLOSED):
            break
        if handle_glass_hover_event(win_prog, ev):
            continue
        if ev == "Start":
            set_glass_button_disabled(win_prog, "-CNL-", False)
            set_glass_button_disabled(win_prog, "Start", True)
            fn = concatenate_rows_gui if axis_mode == "ROWS" else concatenate_columns_gui
            if not fn(win_prog, [csv1, csv2], dst_csv, header=has_header):
                psg.popup("Stopped", "CSV merge did not complete.", title="Stopped")
            break
    win_prog.close()


# =====================================================================
# MAIN DASHBOARD APPLICATION (4-TAB STUDIO WITH GLASS BUTTONS)
# =====================================================================
def build_main_window():
    reg = {}
    tab_w = 212
    tab_specs = [
        ("-TAB-BTN-1-", "1. Fragment Extractor", "folder"),
        ("-TAB-BTN-2-", "2. Feature Studio",      "bolt"),
        ("-TAB-BTN-3-", "3. Detection Methods",   "shield"),
        ("-TAB-BTN-4-", "4. Combine Datasets",    "link"),
    ]
    tab_state_assets = {
        t_key: {
            mode: {st: GLASS.render(t_label, tab_w, 42, pal, st, BG_CARD_TOP, 11, 11, t_icon) for st in ("normal", "hover", "disabled")}
            for mode, pal in (("active", "emerald"), ("inactive", "smoked"))
        }
        for t_key, t_label, t_icon in tab_specs
    }

    header_banner = psg.Column([
        [psg.Text("AFRL  —  Encrypted Fragment Analysis & Detection Studio", font=FONT_TITLE, text_color=ACCENT_CYAN, background_color=BG_CARD_TOP),
         psg.Push(background_color=BG_CARD_TOP),
         make_glass_btn(reg, "-NAV-GUIDE-", "Guidance", 122, 34, palette="smoked", parent_bg=BG_CARD_TOP, radius=9, font_size=11, icon="book", pad=(4, 4)),
         make_glass_btn(reg, "-NAV-ABOUT-", "About",    104, 34, palette="smoked", parent_bg=BG_CARD_TOP, radius=9, font_size=11, icon="info", pad=(4, 4))],
        [psg.Text("An Open-Source Software Tool for Distinguishing Encrypted File Fragments from High-Entropy Data",
                  font=FONT_SUBTITLE, text_color=TEXT_MUTED, background_color=BG_CARD_TOP)]
    ], key="-CARD-HEADER-", background_color=BG_CARD_TOP, pad=((0, 0), (0, 10)), expand_x=True)

    nav_pill_bar = psg.Column([[
        make_glass_btn(reg, t_key, t_label, tab_w, 42, palette=("emerald" if idx == 0 else "smoked"),
                       parent_bg=BG_CARD_TOP, radius=11, font_size=11, icon=t_icon, pad=(4, 5))
        for idx, (t_key, t_label, t_icon) in enumerate(tab_specs)
    ]], key="-CARD-NAV-", background_color=BG_CARD_TOP, pad=((0, 0), (0, 12)), expand_x=True, element_justification='center')

    # VIEW 1: FRAGMENT EXTRACTOR
    view_frag_layout = [
        [psg.Frame(' 1. Extraction Strategy ', [
            [psg.Radio("Dataset-Wide Random Sampling (Extract N total fragments across whole folder)", "FRAG_MODE", key="-MODE-FOLDER-", default=True, enable_events=True, background_color=BG_CARD, font=FONT_BOLD)],
            [psg.Radio("Per-File Uniform Sampling (Extract M random fragments from each file)", "FRAG_MODE", key="-MODE-FILE-", enable_events=True, background_color=BG_CARD, font=FONT_BOLD)],
            [psg.Radio("Per-File Sampling with Random Padding (Pad smaller files automatically)", "FRAG_MODE", key="-MODE-PAD-", enable_events=True, background_color=BG_CARD, font=FONT_BOLD)],
        ], background_color=BG_CARD, title_color=ACCENT_CYAN, font=FONT_SECTION, expand_x=True, pad=(6, 8), relief=psg.RELIEF_FLAT)],
        [psg.Frame(' 2. Parameters & Directories ', [
            [psg.Text("Source Folder:", size=(18, 1), background_color=BG_CARD, font=FONT_BOLD),
             psg.Input(key="-FRAG-SRC-", readonly=True, disabled_readonly_background_color=BG_INPUT, disabled_readonly_text_color=TEXT_PRIMARY, expand_x=True),
             make_glass_btn(reg, "-BRW-FRAG-SRC-", "Choose Folder", 156, 36, palette="smoked", parent_bg=BG_CARD, radius=9, font_size=11, icon="folder", pad=(6, 4), btn_kind="folder")],
            [psg.Text("Fragment Size (Bytes):", size=(18, 1), background_color=BG_CARD, font=FONT_BOLD),
             psg.Input("4096", key="-FRAG-SIZE-", size=(16, 1)),
             psg.Text("Any size supported (e.g., 256, 512, 1024, 2048, 4096, 8192+)", text_color=TEXT_MUTED, background_color=BG_CARD, font=FONT_SUBTITLE)],
            [psg.Text("Total Fragments (N):", key="-FRAG-COUNT-LBL-", size=(18, 1), background_color=BG_CARD, font=FONT_BOLD),
             psg.Input("1000", key="-FRAG-COUNT-", size=(16, 1)),
             psg.Text("Number of fragments to slice", text_color=TEXT_MUTED, background_color=BG_CARD, font=FONT_SUBTITLE)],
            [psg.Text("Destination CSV:", size=(18, 1), background_color=BG_CARD, font=FONT_BOLD),
             psg.Input(key="-FRAG-DST-", readonly=True, disabled_readonly_background_color=BG_INPUT, disabled_readonly_text_color=TEXT_PRIMARY, expand_x=True),
             make_glass_btn(reg, "-SAV-FRAG-DST-", "Save As CSV", 156, 36, palette="smoked", parent_bg=BG_CARD, radius=9, font_size=11, icon="save", pad=(6, 4), btn_kind="save")],
        ], background_color=BG_CARD, title_color=ACCENT_CYAN, font=FONT_SECTION, expand_x=True, pad=(6, 8), relief=psg.RELIEF_FLAT)],
        [psg.VPush()],
        [psg.Push(), make_glass_btn(reg, "-BTN-RUN-FRAG-", "Extract Fragments Now", 290, 46, palette="emerald", parent_bg=BG_MAIN, radius=12, font_size=13, icon="play", pad=(8, 12)), psg.Push()],
        [psg.VPush()]
    ]

    # VIEW 2: FEATURE EXTRACTOR
    notice_text = (
        "All 36 statistical suites (all 15 NIST SP 800-22 tests, HEDGE, BLFCP/EnCoD, AFRL, and SOTA 2026)\n"
        "dynamically adapt their block dimensions, degrees of freedom, and thresholds to ANY fragment length."
    )
    view_feat_layout = [
        [psg.Frame(' Universal Any-Length Adaptive Feature Engine ', [[psg.Text(notice_text, text_color=ACCENT_EMERALD, background_color=BG_CARD, font=FONT_BOLD, pad=(8, 6))]],
                   background_color=BG_CARD, title_color=ACCENT_EMERALD, font=FONT_SECTION, expand_x=True, pad=(6, 8), relief=psg.RELIEF_FLAT)],
        [psg.Frame(' Quick Launch Configuration (Optional Pre-fill) ', [
            [psg.Text("Input Fragments CSV:", size=(18, 1), background_color=BG_CARD, font=FONT_BOLD),
             psg.Input(key="-QUICK-FEAT-IN-", readonly=True, disabled_readonly_background_color=BG_INPUT, disabled_readonly_text_color=TEXT_PRIMARY, expand_x=True),
             make_glass_btn(reg, "-BRW-QFEAT-IN-", "Browse CSV", 156, 36, palette="smoked", parent_bg=BG_CARD, radius=9, font_size=11, icon="folder", pad=(6, 4), btn_kind="file")],
            [psg.Text("Output Features CSV:", size=(18, 1), background_color=BG_CARD, font=FONT_BOLD),
             psg.Input(key="-QUICK-FEAT-OUT-", readonly=True, disabled_readonly_background_color=BG_INPUT, disabled_readonly_text_color=TEXT_PRIMARY, expand_x=True),
             make_glass_btn(reg, "-SAV-QFEAT-OUT-", "Save As CSV", 156, 36, palette="smoked", parent_bg=BG_CARD, radius=9, font_size=11, icon="save", pad=(6, 4), btn_kind="save")],
        ], background_color=BG_CARD, title_color=ACCENT_CYAN, font=FONT_SECTION, expand_x=True, pad=(6, 8), relief=psg.RELIEF_FLAT)],
        [psg.VPush()],
        [psg.Push(), make_glass_btn(reg, "-BTN4-", "Open Feature Selection Studio  (36 Suites)", 410, 46, palette="emerald", parent_bg=BG_MAIN, radius=12, font_size=13, icon="sliders", pad=(8, 12)), psg.Push()],
        [psg.VPush()]
    ]

    # VIEW 3: ENCRYPTED FRAGMENT DETECTION METHODS (AFRL, HEDGE & SOTA '26)
    view_detect_layout = [
        [psg.Frame(' 1. Select Encrypted Fragment Detection Method ', [
            [psg.Radio("AFRL Detector — Adaptive Nibble Variance & NIST Statistical Ensemble (1 = Encrypted, 0 = Unencrypted)",
                       "DETECT_METHOD", key="-DET-METHOD-AFRL-", default=True, enable_events=True, background_color=BG_CARD, font=FONT_BOLD)],
            [psg.Radio("HEDGE Detector — Chi-Square Absolute/Confidence Bands & NIST Ensemble (1 = Encrypted, 0 = Unencrypted)",
                       "DETECT_METHOD", key="-DET-METHOD-HEDGE-", default=False, enable_events=True, background_color=BG_CARD, font=FONT_BOLD)],
            [psg.Radio("SOTA '26 Detector — IoC, Wasserstein, Markov Bigrams, Spectral & Recompression (1 = Encrypted, 0 = Unencrypted)",
                       "DETECT_METHOD", key="-DET-METHOD-SOTA-", default=False, enable_events=True, background_color=BG_CARD, font=FONT_BOLD)],
            [psg.Text(DETECTION_METHODS_REGISTRY["AFRL"]["description"], key="-DET-METHOD-DESC-", font=FONT_SUBTITLE, text_color=TEXT_MUTED, background_color=BG_CARD, pad=((26, 6), (4, 6)))],
        ], background_color=BG_CARD, title_color=ACCENT_CYAN, font=FONT_SECTION, expand_x=True, pad=(6, 6), relief=psg.RELIEF_FLAT)],
        [psg.Frame(' 2. Input Fragments & Output Predictions CSV ', [
            [psg.Text("Fragments CSV:", size=(16, 1), background_color=BG_CARD, font=FONT_BOLD),
             psg.Input(key="-DET-IN-", readonly=True, disabled_readonly_background_color=BG_INPUT, disabled_readonly_text_color=TEXT_PRIMARY, expand_x=True),
             make_glass_btn(reg, "-BRW-DET-IN-", "Browse CSV", 156, 36, palette="smoked", parent_bg=BG_CARD, radius=9, font_size=11, icon="folder", pad=(6, 4), btn_kind="file")],
            [psg.Text("Predictions Output:", size=(16, 1), background_color=BG_CARD, font=FONT_BOLD),
             psg.Input(key="-DET-OUT-", readonly=True, disabled_readonly_background_color=BG_INPUT, disabled_readonly_text_color=TEXT_PRIMARY, expand_x=True),
             make_glass_btn(reg, "-SAV-DET-OUT-", "Save As CSV", 156, 36, palette="smoked", parent_bg=BG_CARD, radius=9, font_size=11, icon="save", pad=(6, 4), btn_kind="save")],
            make_cpu_slider_row("-DET-CPU-", "-DET-CPU-BADGE-", default_val=80, bg_hex=BG_CARD),
            [psg.Checkbox("Include column header ('AFRL_encrypted') in output CSV", default=False, key="-DET-HEADER-", background_color=BG_CARD, font=FONT_BOLD, pad=((6, 0), (4, 4)))]
        ], background_color=BG_CARD, title_color=ACCENT_CYAN, font=FONT_SECTION, expand_x=True, pad=(6, 6), relief=psg.RELIEF_FLAT)],
        [psg.VPush()],
        [psg.Push(), make_glass_btn(reg, "-BTN-RUN-DET-", "Run Detection Method  (0 / 1)", 320, 46, palette="emerald", parent_bg=BG_MAIN, radius=12, font_size=13, icon="shield", pad=(8, 10)), psg.Push()],
        [psg.VPush()]
    ]

    # VIEW 4: CSV DATASET COMBINER
    view_combine_layout = [
        [psg.Frame(' 1. Merge Direction ', [
            [psg.Radio("Stack Vertically Along Rows (Axis 0 — Combine two fragment/feature datasets with identical columns)", "CMB_MODE", key="-CMB-ROWS-", default=True, background_color=BG_CARD, font=FONT_BOLD)],
            [psg.Radio("Join Side-by-Side Along Columns (Axis 1 — Combine different feature sets for the same fragments)", "CMB_MODE", key="-CMB-COLS-", background_color=BG_CARD, font=FONT_BOLD)],
        ], background_color=BG_CARD, title_color=ACCENT_CYAN, font=FONT_SECTION, expand_x=True, pad=(6, 8), relief=psg.RELIEF_FLAT)],
        [psg.Frame(' 2. Source & Destination CSV Files ', [
            [psg.Text("First CSV File:", size=(16, 1), background_color=BG_CARD, font=FONT_BOLD),
             psg.Input(key="-CMB-CSV1-", readonly=True, disabled_readonly_background_color=BG_INPUT, disabled_readonly_text_color=TEXT_PRIMARY, expand_x=True),
             make_glass_btn(reg, "-BRW-CMB-1-", "Browse CSV", 156, 36, palette="smoked", parent_bg=BG_CARD, radius=9, font_size=11, icon="folder", pad=(6, 4), btn_kind="file")],
            [psg.Text("Second CSV File:", size=(16, 1), background_color=BG_CARD, font=FONT_BOLD),
             psg.Input(key="-CMB-CSV2-", readonly=True, disabled_readonly_background_color=BG_INPUT, disabled_readonly_text_color=TEXT_PRIMARY, expand_x=True),
             make_glass_btn(reg, "-BRW-CMB-2-", "Browse CSV", 156, 36, palette="smoked", parent_bg=BG_CARD, radius=9, font_size=11, icon="folder", pad=(6, 4), btn_kind="file")],
            [psg.Text("Merged Output:", size=(16, 1), background_color=BG_CARD, font=FONT_BOLD),
             psg.Input(key="-CMB-DST-", readonly=True, disabled_readonly_background_color=BG_INPUT, disabled_readonly_text_color=TEXT_PRIMARY, expand_x=True),
             make_glass_btn(reg, "-SAV-CMB-DST-", "Save As CSV", 156, 36, palette="smoked", parent_bg=BG_CARD, radius=9, font_size=11, icon="save", pad=(6, 4), btn_kind="save")],
            [psg.Checkbox("Input CSV files contain a header row", default=True, key="-CMB-HEADER-", background_color=BG_CARD, font=FONT_BOLD, pad=((6, 0), (8, 4)))]
        ], background_color=BG_CARD, title_color=ACCENT_CYAN, font=FONT_SECTION, expand_x=True, pad=(6, 8), relief=psg.RELIEF_FLAT)],
        [psg.VPush()],
        [psg.Push(), make_glass_btn(reg, "-BTN-RUN-CMB-", "Combine CSV Datasets Now", 290, 46, palette="emerald", parent_bg=BG_MAIN, radius=12, font_size=13, icon="link", pad=(8, 12)), psg.Push()],
        [psg.VPush()]
    ]

    status_footer = psg.Column([[
        psg.Text(f"● System Ready   │   Detected {mp.cpu_count()} Logical CPU Cores   │   36 Feature Suites + AFRL, HEDGE & SOTA '26 Detectors Ready",
                 font=FONT_SUBTITLE, text_color=TEXT_MUTED, background_color=BG_CARD_TOP),
        psg.Push(background_color=BG_CARD_TOP),
        psg.Text("Studio v1.1.0", font=FONT_SUBTITLE, text_color=ACCENT_CYAN, background_color=BG_CARD_TOP),
    ]], key="-CARD-FOOTER-", background_color=BG_CARD_TOP, pad=((0, 0), (10, 0)), expand_x=True)

    main_layout = [
        [header_banner],
        [nav_pill_bar],
        [
            psg.Column(view_frag_layout,    key="-VIEW-1-", visible=True,  expand_x=True, expand_y=True, pad=(0, 0)),
            psg.Column(view_feat_layout,    key="-VIEW-2-", visible=False, expand_x=True, expand_y=True, pad=(0, 0)),
            psg.Column(view_detect_layout,  key="-VIEW-3-", visible=False, expand_x=True, expand_y=True, pad=(0, 0)),
            psg.Column(view_combine_layout, key="-VIEW-4-", visible=False, expand_x=True, expand_y=True, pad=(0, 0)),
        ],
        [status_footer],
    ]

    window = psg.Window('AFRL', main_layout, size=(960, 715), resizable=True, finalize=True)
    set_window_icon(window)
    det_cpu_state = {"-DET-CPU-": 80}
    det_cpu_slider = SleekCpuSlider(window["-DET-CPU-"], window["-DET-CPU-BADGE-"], det_cpu_state, "-DET-CPU-", default_val=80, bg_hex=BG_CARD)
    window._det_cpu_state = det_cpu_state
    window._det_cpu_slider = det_cpu_slider
    finalize_sleek_window(window, reg, {"-CARD-HEADER-", "-CARD-NAV-", "-CARD-FOOTER-"})
    det_cpu_slider.redraw()
    return window, tab_state_assets


def _prewarm_studio_assets():
    """Pre-renders Feature Studio glass buttons in background so opening Studio is instantaneous."""
    specs = [
        ("Select All", 118, 34, "smoked", BG_CARD, 9, 11, "check"),
        ("Clear All",  110, 34, "smoked", BG_CARD, 9, 11, "close"),
        ("AFRL",        96, 34, "indigo", BG_CARD, 9, 11, "star"),
        ("HEDGE",      104, 34, "indigo", BG_CARD, 9, 11, "star"),
        ("BLFCP",      102, 34, "indigo", BG_CARD, 9, 11, "star"),
        ("EnCoD",      104, 34, "indigo", BG_CARD, 9, 11, "star"),
        ("SOTA '26",   114, 34, "indigo", BG_CARD, 9, 11, "star"),
        ("All NIST",   112, 34, "cyan",   BG_CARD, 9, 11, "bolt"),
        ("Browse CSV", 142, 34, "smoked", BG_CARD, 9, 11, "folder"),
        ("Save As CSV",142, 34, "smoked", BG_CARD, 9, 11, "save"),
        ("Cancel",     140, 40, "smoked", BG_MAIN, 10, 12, "close"),
        ("Start Feature Extraction", 260, 40, "emerald", BG_MAIN, 10, 12, "play"),
    ]
    for txt, w, h, pal, bg, r, fs, ic in specs:
        for st in ("normal", "hover", "disabled"):
            GLASS.render(txt, w, h, pal, st, bg, r, fs, ic)


if __name__ == '__main__':
    mp.freeze_support()
    mp.set_start_method('spawn', force=True)

    window, tab_state_assets = build_main_window()
    threading.Thread(target=_prewarm_studio_assets, daemon=True).start()

    def switch_tab(target_idx):
        reg = getattr(window, "_glass_registry", {})
        for idx in (1, 2, 3, 4):
            is_active = (idx == target_idx)
            t_key = f"-TAB-BTN-{idx}-"
            window[f"-VIEW-{idx}-"].update(visible=is_active)
            mode_imgs = tab_state_assets[t_key]["active" if is_active else "inactive"]
            reg[t_key]["imgs"] = mode_imgs
            window[t_key].update(image_data=mode_imgs["normal"])
        if target_idx == 3 and hasattr(window, "_det_cpu_slider"):
            window._det_cpu_slider.redraw()

    while True:
        event, values = window.read()
        if event in ("Exit", psg.WIN_CLOSED):
            break
        if handle_glass_hover_event(window, event):
            continue

        if event in ("-TAB-BTN-1-", "-TAB-BTN-2-", "-TAB-BTN-3-", "-TAB-BTN-4-"):
            switch_tab(int(event.split("-")[-2]))
        elif event == "-NAV-ABOUT-":
            show_about_modal()
        elif event == "-NAV-GUIDE-":
            show_guidance_modal()
        elif event == "-MODE-FOLDER-":
            window["-FRAG-COUNT-LBL-"].update("Total Fragments (N):")
        elif event in ("-MODE-FILE-", "-MODE-PAD-"):
            window["-FRAG-COUNT-LBL-"].update("Fragments / File (M):")
        elif event == '-BTN-RUN-FRAG-':
            mode = "FOLDER_RANDOM" if values.get("-MODE-FOLDER-") else ("FILE_RANDOM" if values.get("-MODE-FILE-") else "FILE_PADDED")
            run_fragment_extraction_workflow(
                mode,
                values.get("-FRAG-SRC-", "").strip(),
                values.get("-FRAG-SIZE-", "4096").strip(),
                values.get("-FRAG-COUNT-", "1000").strip(),
                values.get("-FRAG-DST-", "").strip(),
            )
        elif event == '-BTN4-':
            open_feature_extractor_studio(
                input_csv_path=values.get("-QUICK-FEAT-IN-", "").strip(),
                output_csv_path=values.get("-QUICK-FEAT-OUT-", "").strip(),
            )
        elif event in ("-DET-METHOD-AFRL-", "-DET-METHOD-HEDGE-", "-DET-METHOD-SOTA-"):
            sel_method = "SOTA" if values.get("-DET-METHOD-SOTA-") else ("HEDGE" if values.get("-DET-METHOD-HEDGE-") else "AFRL")
            info = DETECTION_METHODS_REGISTRY[sel_method]
            window["-DET-METHOD-DESC-"].update(info["description"])
            window["-DET-HEADER-"].update(text=f"  Include column header ('{info['header']}') in output CSV")
        elif event == '-BTN-RUN-DET-':
            method_key = "SOTA" if values.get("-DET-METHOD-SOTA-") else ("HEDGE" if values.get("-DET-METHOD-HEDGE-") else "AFRL")
            run_detection_method_workflow(
                method_key,
                values.get("-DET-IN-", "").strip(),
                values.get("-DET-OUT-", "").strip(),
                window._det_cpu_state.get("-DET-CPU-", 80),
                values.get("-DET-HEADER-", False),
            )
        elif event == '-BTN-RUN-CMB-':
            run_csv_combine_workflow(
                "ROWS" if values.get("-CMB-ROWS-") else "COLS",
                values.get("-CMB-CSV1-", "").strip(),
                values.get("-CMB-CSV2-", "").strip(),
                values.get("-CMB-HEADER-", True),
                values.get("-CMB-DST-", "").strip(),
            )

    window.close()