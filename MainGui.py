import FreeSimpleGUI as psg
import sys
import os
import numpy as np
import pandas as pd
import multiprocessing as mp
import time
import threading
from functools import partial
from math import ceil
from PIL import Image, ImageTk


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


sys.path.append(resource_path("fragment_extractor"))
sys.path.append(resource_path("feature_extractor"))
sys.path.append(resource_path(os.path.join("feature_extractor", "NIST")))
sys.path.append(resource_path(os.path.join("feature_extractor", "HEDGE_specials")))
sys.path.append(resource_path(os.path.join("feature_extractor", "BLFCP_specials")))
sys.path.append(resource_path(os.path.join("feature_extractor", "SOTA_specials")))
sys.path.append(resource_path(os.path.join("feature_extractor", "AFRL_specials")))
sys.path.append(resource_path("combine_csv"))

from extract_whole_folder_fragments import extract_whole_folder_fragments
from extract_each_file_fragments import extract_random_fragments_file
from extract_each_file_fragments import extract_random_fragments_file_random_pad
from sp800_22_longest_run_ones_in_a_block_test import longest_run_ones_in_a_block_test
from sp800_22_overlapping_template_matching_test import overlapping_template_matching_test
from sp800_22_approximate_entropy_test import approximate_entropy_test
from sp800_22_frequency_within_block_test import frequency_within_block_test
from sp800_22_runs_test import runs_test
from sp800_22_random_excursion_variant_test import random_excursion_variant_test
from sp800_22_maurers_universal_test import maurers_universal_test
from sp800_22_cumulative_sums_test import cumulative_sums_test
from sp800_22_non_overlapping_template_matching_test import non_overlapping_template_matching_test
from sp800_22_monobit_test import monobit_test
from sp800_22_random_excursion_test import random_excursion_test
from sp800_22_dft_test import dft_test
from sp800_22_serial_test import serial_test
from sp800_22_binary_matrix_rank_test import binary_matrix_rank_test
from sp800_22_linear_complexity_test import linear_complexity_test
from HEDGE_fspecials import *
from BLFCP_fspecials import *
from SOTA_fspecials import *
from AFRL_fspecials import *
from combine import *


# =====================================================================
# MODERN CYBER-OBSIDIAN THEME & CUSTOM BUTTON / TAB STYLING
# =====================================================================
BG_MAIN        = '#0B0F19'  # Ultra-Deep Obsidian Slate
BG_CARD        = '#151E2E'  # Elevated Surface Card
BG_CARD_ALT    = '#1E293B'  # Highlighted Surface / Nav Bar
BG_INPUT       = '#283548'  # Input Field Slate
BORDER_SUBTLE  = '#334155'  # Subtle Hairline Border
TEXT_PRIMARY   = '#F8FAFC'  # Crisp Alabaster White
TEXT_MUTED     = '#94A3B8'  # Muted Cool Gray
ACCENT_EMERALD = '#10B981'  # Primary Emerald Neon
ACCENT_HOVER   = '#059669'  # Deep Emerald Hover
ACCENT_INDIGO  = '#6366F1'  # Preset Indigo Accent
ACCENT_IND_HOV = '#4F46E5'  # Preset Indigo Hover
ACCENT_AMBER   = '#F59E0B'  # Warning / Highlight Gold
ACCENT_DANGER  = '#EF4444'  # Danger / Cancel Red
ACCENT_DNG_HOV = '#DC2626'  # Danger Hover

psg.LOOK_AND_FEEL_TABLE['CyberObsidian'] = {
    'BACKGROUND': BG_MAIN,
    'TEXT': TEXT_PRIMARY,
    'INPUT': BG_INPUT,
    'TEXT_INPUT': TEXT_PRIMARY,
    'SCROLL': ACCENT_EMERALD,
    'BUTTON': ('#FFFFFF', ACCENT_EMERALD),
    'PROGRESS': (ACCENT_EMERALD, BG_INPUT),
    'BORDER': 0,
    'SLIDER_DEPTH': 0,
    'PROGRESS_DEPTH': 0,
}

psg.theme('CyberObsidian')
psg.set_options(
    font=('Segoe UI', 10),
    button_color=('#FFFFFF', ACCENT_EMERALD),
    border_width=0,
    element_padding=(6, 6),
    margins=(14, 14)
)

# Color Tuples for Custom States
BTN_PRIMARY    = ('#FFFFFF', ACCENT_EMERALD)
BTN_PRI_HOVER  = ('#FFFFFF', ACCENT_HOVER)

BTN_SECONDARY  = (TEXT_PRIMARY, BG_INPUT)
BTN_SEC_HOVER  = ('#FFFFFF', '#3B4D66')

BTN_PRESET     = ('#FFFFFF', ACCENT_INDIGO)
BTN_PRE_HOVER  = ('#FFFFFF', ACCENT_IND_HOV)

BTN_DANGER     = ('#FFFFFF', ACCENT_DANGER)
BTN_DNG_HOVER  = ('#FFFFFF', ACCENT_DNG_HOV)

TAB_ACTIVE_COLOR   = ('#FFFFFF', ACCENT_EMERALD)
TAB_INACTIVE_COLOR = (TEXT_MUTED, BG_CARD)
TAB_HOVER_COLOR    = (TEXT_PRIMARY, '#263449')

FONT_TITLE     = ('Segoe UI Semibold', 15)
FONT_SUBTITLE  = ('Segoe UI', 9)
FONT_SECTION   = ('Segoe UI Semibold', 11)
FONT_TAB       = ('Segoe UI Semibold', 11)
FONT_BTN_LG    = ('Segoe UI Semibold', 11)
FONT_BOLD      = ('Segoe UI Semibold', 10)
FONT_MONO      = ('Consolas', 9)


def polish_window_widgets(win, hover_map=None):
    """
    Applies native Tkinter cursor='hand2', flat borders, and smooth hover transitions
    to buttons so they feel like modern web/desktop UI components.
    """
    if hover_map is None:
        hover_map = {}

    for _, elem in win.key_dict.items():
        if isinstance(elem, psg.Button):
            try:
                w = elem.Widget
                w.configure(
                    cursor="hand2",
                    relief="flat",
                    bd=0,
                    highlightthickness=0,
                    activebackground=ACCENT_HOVER,
                    activeforeground="#FFFFFF",
                )
            except Exception:
                pass

    for btn_key in hover_map:
        if btn_key in win.key_dict:
            win[btn_key].bind('<Enter>', '_HOVER_IN')
            win[btn_key].bind('<Leave>', '_HOVER_OUT')


# =====================================================================
# FEATURE PRESETS & DEFINITIONS (NIST + HEDGE + BLFCP + SOTA + AFRL)
# =====================================================================
ALL_FEATURE_KEYS = [
    # NIST SP 800-22 (1..15)
    '1_1_longest_run_ones_in_a_block_test_success', '1_2_longest_run_ones_in_a_block_test_pvalue',
    '2_1_overlapping_template_matching_test_success', '2_2_overlapping_template_matching_test_pvalue',
    '3_1_approximate_entropy_test_success', '3_2_approximate_entropy_test_pvalue',
    '4_1_frequency_within_block_test_success', '4_2_frequency_within_block_test_pvalue',
    '5_1_runs_test_success', '5_2_runs_test_pvalue',
    '6_1_random_excursion_variant_test_success', '6_2_random_excursion_variant_test_pvalue',
    '7_1_maurers_universal_test_success', '7_2_maurers_universal_test_pvalue',
    '8_1_cumulative_sums_test_success', '8_2_cumulative_sums_test_pvalue',
    '9_1_non_overlapping_template_matching_test_success', '9_2_non_overlapping_template_matching_test_pvalue',
    '10_1_monobit_test_success', '10_2_monobit_test_pvalue',
    '11_1_random_excursion_test_success', '11_2_random_excursion_test_pvalue',
    '12_1_dft_test_success', '12_2_dft_test_pvalue',
    '13_1_serial_test_success', '13_2_serial_test_pvalue',
    '14_1_binary_matrix_rank_test_success', '14_2_binary_matrix_rank_test_pvalue',
    '15_1_linear_complexity_test_success', '15_2_linear_complexity_test_pvalue',
    # HEDGE Chi-Square Suite (16..18)
    '16_chi_test_absolute_success', '17_chi_test_confidence_success',
    '18_1_chi2_uniform_stat', '18_2_chi2_uniform_pvalue',
    # BLFCP & EnCoD Suite (19..22)
    '19_byte_frequency_distribution', '20_discrete_fourier_transform',
    '21_lempel_ziv_complexity', '22_monte_carlo_pi_approximation',
    # SOTA Multi-Modal, Markov & Spectral Suite 2024-2026 (23..34)
    '23_byte_moments',
    '24_ks_uniformity',
    '25_wasserstein_distance_uniform',
    '26_index_of_coincidence',
    '27_generalized_entropies',
    '28_serial_and_lag_autocorr',
    '29_markov_bigram_metrics',
    '30_byte_difference_stats',
    '31_recompression_ratios',
    '32_haar_wavelet_detail_ratio',
    '33_spectral_flatness_wiener',
    '34_structural_run_and_ascii',
    # AFRL Custom Suite (35..36)
    '35_1_local_nibbles_variance_success',
    '35_2_local_nibbles_variance_stat',
    '36_afrl_combined_success',
]

DISABLED_NIST_KEYS = {
    '2_1_overlapping_template_matching_test_success', '2_2_overlapping_template_matching_test_pvalue',
    '7_1_maurers_universal_test_success', '7_2_maurers_universal_test_pvalue',
    '15_1_linear_complexity_test_success', '15_2_linear_complexity_test_pvalue'
}

PRESET_HEDGE = {
    '3_1_approximate_entropy_test_success',
    '4_1_frequency_within_block_test_success',
    '8_1_cumulative_sums_test_success',
    '16_chi_test_absolute_success',
    '17_chi_test_confidence_success'
}

PRESET_ENCOD = {
    '19_byte_frequency_distribution'
}

PRESET_BLFCP = {
    '18_1_chi2_uniform_stat',
    '18_2_chi2_uniform_pvalue',
    '19_byte_frequency_distribution',
    '20_discrete_fourier_transform',
    '21_lempel_ziv_complexity',
    '22_monte_carlo_pi_approximation'
}

PRESET_SOTA = {
    '23_byte_moments',
    '24_ks_uniformity',
    '25_wasserstein_distance_uniform',
    '26_index_of_coincidence',
    '27_generalized_entropies',
    '28_serial_and_lag_autocorr',
    '29_markov_bigram_metrics',
    '30_byte_difference_stats',
    '31_recompression_ratios',
    '32_haar_wavelet_detail_ratio',
    '33_spectral_flatness_wiener',
    '34_structural_run_and_ascii',
}

PRESET_AFRL = {
    '3_1_approximate_entropy_test_success',
    '4_1_frequency_within_block_test_success',
    '5_1_runs_test_success',
    '35_1_local_nibbles_variance_success',
    '35_2_local_nibbles_variance_stat',
    '36_afrl_combined_success',
}


# =====================================================================
# DIALOGS & MODALS
# =====================================================================
def set_window_icon(win):
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        icon_path = os.path.join(script_dir, "icons", "main_icon.png")
        if os.path.exists(icon_path):
            image = Image.open(icon_path)
            icon_photo = ImageTk.PhotoImage(image)
            win.TKroot.icon_photo = icon_photo
            win.TKroot.iconphoto(False, icon_photo)
    except Exception:
        pass


def show_guidance_modal():
    guide_lines = [
        "WORKFLOW OVERVIEW",
        "────────────────────────────────────────────────────────────",
        "1. Fragment Extraction:",
        "   Select a dataset folder and extract fixed-size byte fragments into a CSV file.",
        "   • 4096-byte fragments are strongly recommended as the standard benchmark size.",
        "",
        "2. Feature Extraction:",
        "   Load the extracted fragment CSV, select individual features or benchmark presets,",
        "   configure CPU core allocation, and export the computed feature matrix.",
        "   • Features ending with '_success' are binary indicators (1 = Encrypted/Random, 0 = Unencrypted).",
        "",
        "BENCHMARK FEATURE PRESETS",
        "────────────────────────────────────────────────────────────",
        "• AFRL Full Suite (Custom Method):",
        "  - 3_1_approximate_entropy_test_success",
        "  - 4_1_frequency_within_block_test_success",
        "  - 5_1_runs_test_success",
        "  - 35_1_local_nibbles_variance_success (Threshold <= 46.98)",
        "  - 35_2_local_nibbles_variance_stat (Sliding-window 4-bit nibble Chi2 variance)",
        "  - 36_afrl_combined_success (Ensemble decision: AppEn & BlockFreq & Runs & LNV)",
        "",
        "• HEDGE Full Suite:",
        "  - 3_1_approximate_entropy_test_success",
        "  - 4_1_frequency_within_block_test_success",
        "  - 8_1_cumulative_sums_test_success",
        "  - 16_chi_test_absolute_success",
        "  - 17_chi_test_confidence_success",
        "",
        "• EnCoD Suite:",
        "  - 19_byte_frequency_distribution (256 features)",
        "",
        "• BLFCP Full Suite:",
        "  - 18_1_chi2_uniform_stat & 18_2_chi2_uniform_pvalue",
        "  - 19_byte_frequency_distribution (256 features)",
        "  - 20_discrete_fourier_transform (10 features)",
        "  - 21_lempel_ziv_complexity",
        "  - 22_monte_carlo_pi_approximation",
        "",
        "• SOTA 2024–2026 Multi-Modal & Markov Suite (26 columns):",
        "  - 23_byte_moments (Mean, Variance, Skewness, Kurtosis)",
        "  - 24_ks_uniformity & 25_wasserstein_distance_uniform",
        "  - 26_index_of_coincidence & 27_generalized_entropies (Rényi H2, Min-Entropy, Tsallis)",
        "  - 28_serial_and_lag_autocorr (SCC + Lags 2, 4, 8)",
        "  - 29_markov_bigram_metrics (Bigram Entropy, Conditional Entropy, Mutual Info, Unique Ratio)",
        "  - 30_byte_difference_stats & 31_recompression_ratios (zlib, bz2, lzma)",
        "  - 32_haar_wavelet_detail_ratio, 33_spectral_flatness_wiener & 34_structural_run_and_ascii"
    ]
    guide_text = chr(10).join(guide_lines)
    layout = [
        [psg.Text("📖 User Guide & Benchmark Reference", font=FONT_TITLE, text_color=ACCENT_EMERALD)],
        [psg.Multiline(guide_text, size=(76, 25), font=FONT_MONO, disabled=True,
                       background_color=BG_CARD, text_color=TEXT_PRIMARY, border_width=0, pad=(6, 8))],
        [psg.Push(), psg.Button("✓  Got It", key="-CLOSE-", size=(14, 1), font=FONT_BOLD, button_color=BTN_PRIMARY, pad=(4, 8)), psg.Push()]
    ]
    win = psg.Window("Guidance & Documentation", layout, modal=True, finalize=True)
    set_window_icon(win)
    polish_window_widgets(win, {"-CLOSE-": (BTN_PRIMARY, BTN_PRI_HOVER)})
    while True:
        ev, _ = win.read()
        if ev in (psg.WIN_CLOSED, "-CLOSE-"):
            break
        elif ev == "-CLOSE-_HOVER_IN":
            win["-CLOSE-"].update(button_color=BTN_PRI_HOVER)
        elif ev == "-CLOSE-_HOVER_OUT":
            win["-CLOSE-"].update(button_color=BTN_PRIMARY)
    win.close()


def show_about_modal():
    desc_text = chr(10).join([
        "High-performance fragment slicer and statistical feature extraction suite",
        "for encrypted vs. unencrypted data classification (NIST SP 800-22, AFRL, HEDGE, BLFCP, EnCoD, SOTA 2026)."
    ])
    layout = [
        [psg.Column([
            [psg.Text("🛡️ Enc Fragment Extractor", font=FONT_TITLE, text_color=ACCENT_EMERALD, background_color=BG_CARD)],
            [psg.Text("Version 0.9.5 — Studio Edition", font=FONT_BOLD, text_color=TEXT_MUTED, background_color=BG_CARD)],
            [psg.HorizontalSeparator(color=BORDER_SUBTLE)],
            [psg.Text(desc_text, font=('Segoe UI', 10), text_color=TEXT_PRIMARY, background_color=BG_CARD)],
            [psg.Text("Created by: Alireza Aliaskari Hosseinabadi", font=FONT_BOLD, background_color=BG_CARD, pad=((0, 0), (10, 4)))],
            [psg.Text("License: GNU General Public License v3.0 (GPL-3.0)", font=FONT_SUBTITLE, text_color=TEXT_MUTED, background_color=BG_CARD)],
            [psg.VPush(background_color=BG_CARD)],
            [psg.Push(background_color=BG_CARD),
             psg.Button("✓  Close", key="-OK-", size=(14, 1), font=FONT_BOLD, button_color=BTN_PRIMARY),
             psg.Push(background_color=BG_CARD)]
        ], background_color=BG_CARD, pad=(18, 18), expand_x=True, expand_y=True)]
    ]
    win = psg.Window("About Application", layout, modal=True, finalize=True)
    set_window_icon(win)
    polish_window_widgets(win, {"-OK-": (BTN_PRIMARY, BTN_PRI_HOVER)})
    while True:
        ev, _ = win.read()
        if ev in (psg.WIN_CLOSED, "-OK-"):
            break
        elif ev == "-OK-_HOVER_IN":
            win["-OK-"].update(button_color=BTN_PRI_HOVER)
        elif ev == "-OK-_HOVER_OUT":
            win["-OK-"].update(button_color=BTN_PRIMARY)
    win.close()


def create_progress_modal(title, subtitle):
    layout = [
        [psg.Column([
            [psg.Text(title, font=FONT_SECTION, text_color=ACCENT_EMERALD, background_color=BG_CARD)],
            [psg.Text(subtitle, font=FONT_SUBTITLE, text_color=TEXT_MUTED, background_color=BG_CARD)],
            [psg.HorizontalSeparator(color=BORDER_SUBTLE)],
            [psg.Text("Status:", font=FONT_BOLD, background_color=BG_CARD, text_color=TEXT_MUTED),
             psg.Text("Ready to start", key='-STATUS-', font=FONT_BOLD, background_color=BG_CARD, text_color=TEXT_PRIMARY, size=(42, 1))],
            [psg.ProgressBar(100, orientation='h', size=(48, 22), key='-PROGRESS-')],
            [psg.Text("Elapsed: 00:00:00  |  ETR: --:--:--", key='-TIME-', font=FONT_MONO, background_color=BG_CARD, text_color=ACCENT_AMBER)],
            [psg.Push(background_color=BG_CARD),
             psg.Button("▶  Start Execution", key="Start", size=(16, 1), font=FONT_BOLD, button_color=BTN_PRIMARY, pad=(6, 10)),
             psg.Button("✖  Cancel", key="-CNL-", size=(14, 1), font=FONT_BOLD, button_color=BTN_DANGER, disabled=True, pad=(6, 10)),
             psg.Push(background_color=BG_CARD)]
        ], background_color=BG_CARD, pad=(18, 18), expand_x=True)]
    ]
    win = psg.Window(title, layout, modal=True, finalize=True)
    set_window_icon(win)
    polish_window_widgets(win, {
        "Start": (BTN_PRIMARY, BTN_PRI_HOVER),
        "-CNL-": (BTN_DANGER, BTN_DNG_HOVER)
    })
    return win


# =====================================================================
# CORE HELPER & WORKER FUNCTIONS
# =====================================================================
def byte_to_bit(row):
    bit_list = [format(int(num), '08b') for num in row]
    bit_s_list = []
    for bit_8 in bit_list:
        for bit in bit_8:
            bit_s_list.append(int(bit))
    return bit_s_list


def format_time(seconds):
    if seconds is None or seconds < 0:
        return "Calculating..."
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def process_row(row, dic_checklist):
    row_np = np.asarray(row, dtype=np.uint8)
    bitn_list = [
        '1_1_longest_run_ones_in_a_block_test_success', '1_2_longest_run_ones_in_a_block_test_pvalue',
        '2_1_overlapping_template_matching_test_success', '2_2_overlapping_template_matching_test_pvalue',
        '3_1_approximate_entropy_test_success', '3_2_approximate_entropy_test_pvalue',
        '4_1_frequency_within_block_test_success', '4_2_frequency_within_block_test_pvalue',
        '5_1_runs_test_success', '5_2_runs_test_pvalue',
        '6_1_random_excursion_variant_test_success', '6_2_random_excursion_variant_test_pvalue',
        '7_1_maurers_universal_test_success', '7_2_maurers_universal_test_pvalue',
        '8_1_cumulative_sums_test_success', '8_2_cumulative_sums_test_pvalue',
        '9_1_non_overlapping_template_matching_test_success', '9_2_non_overlapping_template_matching_test_pvalue',
        '10_1_monobit_test_success', '10_2_monobit_test_pvalue',
        '11_1_random_excursion_test_success', '11_2_random_excursion_test_pvalue',
        '12_1_dft_test_success', '12_2_dft_test_pvalue',
        '13_1_serial_test_success', '13_2_serial_test_pvalue',
        '14_1_binary_matrix_rank_test_success', '14_2_binary_matrix_rank_test_pvalue',
        '15_1_linear_complexity_test_success', '15_2_linear_complexity_test_pvalue'
    ]
    bitn_values = [dic_checklist.get(k, False) for k in bitn_list]
    if any(bitn_values):
        bits = byte_to_bit(row_np)

    all_features = []

    # --- NIST SP 800-22 Features (1..15) ---
    if dic_checklist.get('1_1_longest_run_ones_in_a_block_test_success') or dic_checklist.get('1_2_longest_run_ones_in_a_block_test_pvalue'):
        success, pval, _ = longest_run_ones_in_a_block_test(bits)
        if dic_checklist.get('1_1_longest_run_ones_in_a_block_test_success'): all_features.append(1 if success else 0)
        if dic_checklist.get('1_2_longest_run_ones_in_a_block_test_pvalue'): all_features.append(pval)
    if dic_checklist.get('2_1_overlapping_template_matching_test_success') or dic_checklist.get('2_2_overlapping_template_matching_test_pvalue'):
        success, pval, _ = overlapping_template_matching_test(bits)
        if dic_checklist.get('2_1_overlapping_template_matching_test_success'): all_features.append(1 if success else 0)
        if dic_checklist.get('2_2_overlapping_template_matching_test_pvalue'): all_features.append(pval)
    if dic_checklist.get('3_1_approximate_entropy_test_success') or dic_checklist.get('3_2_approximate_entropy_test_pvalue'):
        success, pval, _ = approximate_entropy_test(bits)
        if dic_checklist.get('3_1_approximate_entropy_test_success'): all_features.append(1 if success else 0)
        if dic_checklist.get('3_2_approximate_entropy_test_pvalue'): all_features.append(pval)
    if dic_checklist.get('4_1_frequency_within_block_test_success') or dic_checklist.get('4_2_frequency_within_block_test_pvalue'):
        success, pval, _ = frequency_within_block_test(bits)
        if dic_checklist.get('4_1_frequency_within_block_test_success'): all_features.append(1 if success else 0)
        if dic_checklist.get('4_2_frequency_within_block_test_pvalue'): all_features.append(pval)
    if dic_checklist.get('5_1_runs_test_success') or dic_checklist.get('5_2_runs_test_pvalue'):
        success, pval, _ = runs_test(bits)
        if dic_checklist.get('5_1_runs_test_success'): all_features.append(1 if success else 0)
        if dic_checklist.get('5_2_runs_test_pvalue'): all_features.append(pval)
    if dic_checklist.get('6_1_random_excursion_variant_test_success') or dic_checklist.get('6_2_random_excursion_variant_test_pvalue'):
        success, _, *pval = random_excursion_variant_test(bits)
        if dic_checklist.get('6_1_random_excursion_variant_test_success'): all_features.append(1 if success else 0)
        if dic_checklist.get('6_2_random_excursion_variant_test_pvalue'):
            for pv in pval[0]: all_features.append(pv)
    if dic_checklist.get('8_1_cumulative_sums_test_success') or dic_checklist.get('8_2_cumulative_sums_test_pvalue'):
        success, _, *pval = cumulative_sums_test(bits)
        if dic_checklist.get('8_1_cumulative_sums_test_success'): all_features.append(1 if success else 0)
        if dic_checklist.get('8_2_cumulative_sums_test_pvalue'):
            for pv in pval[0]: all_features.append(pv)
    if dic_checklist.get('9_1_non_overlapping_template_matching_test_success') or dic_checklist.get('9_2_non_overlapping_template_matching_test_pvalue'):
        success, pval, _ = non_overlapping_template_matching_test(bits)
        if dic_checklist.get('9_1_non_overlapping_template_matching_test_success'): all_features.append(1 if success else 0)
        if dic_checklist.get('9_2_non_overlapping_template_matching_test_pvalue'): all_features.append(pval)
    if dic_checklist.get('10_1_monobit_test_success') or dic_checklist.get('10_2_monobit_test_pvalue'):
        success, pval, _ = monobit_test(bits)
        if dic_checklist.get('10_1_monobit_test_success'): all_features.append(1 if success else 0)
        if dic_checklist.get('10_2_monobit_test_pvalue'): all_features.append(pval)
    if dic_checklist.get('11_1_random_excursion_test_success') or dic_checklist.get('11_2_random_excursion_test_pvalue'):
        success, _, *pval = random_excursion_test(bits)
        if dic_checklist.get('11_1_random_excursion_test_success'): all_features.append(1 if success else 0)
        if dic_checklist.get('11_2_random_excursion_test_pvalue'):
            for pv in pval[0]: all_features.append(pv)
    if dic_checklist.get('12_1_dft_test_success') or dic_checklist.get('12_2_dft_test_pvalue'):
        success, pval, _ = dft_test(bits)
        if dic_checklist.get('12_1_dft_test_success'): all_features.append(1 if success else 0)
        if dic_checklist.get('12_2_dft_test_pvalue'): all_features.append(pval)
    if dic_checklist.get('13_1_serial_test_success') or dic_checklist.get('13_2_serial_test_pvalue'):
        success, _, *pval = serial_test(bits)
        if dic_checklist.get('13_1_serial_test_success'): all_features.append(1 if success else 0)
        if dic_checklist.get('13_2_serial_test_pvalue'):
            for pv in pval[0]: all_features.append(pv)
    if dic_checklist.get('14_1_binary_matrix_rank_test_success') or dic_checklist.get('14_2_binary_matrix_rank_test_pvalue'):
        success, pval, _ = binary_matrix_rank_test(bits, M=28, Q=28)
        if dic_checklist.get('14_1_binary_matrix_rank_test_success'): all_features.append(1 if success else 0)
        if dic_checklist.get('14_2_binary_matrix_rank_test_pvalue'): all_features.append(pval)

    # --- HEDGE Chi-Square Features (16..18) ---
    if dic_checklist.get("16_chi_test_absolute_success") or dic_checklist.get("17_chi_test_confidence_success") or dic_checklist.get("18_1_chi2_uniform_stat") or dic_checklist.get("18_2_chi2_uniform_pvalue"):
        chi2_stat, p_value = chi2_uniform(row_np, Length=len(row_np))
        if dic_checklist.get("16_chi_test_absolute_success"): all_features.append(chi_test_absolute(chi2_stat, length=len(row_np)))
        if dic_checklist.get("17_chi_test_confidence_success"): all_features.append(chi_test_confidence(p_value))
        if dic_checklist.get("18_1_chi2_uniform_stat"): all_features.append(chi2_stat)
        if dic_checklist.get("18_2_chi2_uniform_pvalue"): all_features.append(p_value)

    # --- BLFCP & EnCoD Features (19..22) ---
    if dic_checklist.get("19_byte_frequency_distribution"): all_features.extend(byte_frequency_distribution(row_np))
    if dic_checklist.get("20_discrete_fourier_transform"): all_features.extend(discrete_fourier_transform(row_np))
    if dic_checklist.get("21_lempel_ziv_complexity"): all_features.append(lempel_ziv_complexity(row_np))
    if dic_checklist.get("22_monte_carlo_pi_approximation"): all_features.append(monte_carlo_pi_approximation(row_np))

    # --- SOTA 2024-2026 Multi-Modal, Markov & Spectral Features (23..34) ---
    if dic_checklist.get("23_byte_moments"): all_features.extend(byte_statistical_moments(row_np))
    if dic_checklist.get("24_ks_uniformity"): all_features.extend(ks_uniformity_test(row_np))
    if dic_checklist.get("25_wasserstein_distance_uniform"): all_features.append(wasserstein_uniform_distance(row_np))
    if dic_checklist.get("26_index_of_coincidence"): all_features.append(index_of_coincidence(row_np))
    if dic_checklist.get("27_generalized_entropies"): all_features.extend(generalized_entropies(row_np))
    if dic_checklist.get("28_serial_and_lag_autocorr"): all_features.extend(serial_and_lag_autocorrelation(row_np))
    if dic_checklist.get("29_markov_bigram_metrics"): all_features.extend(markov_bigram_features(row_np))
    if dic_checklist.get("30_byte_difference_stats"): all_features.extend(byte_difference_features(row_np))
    if dic_checklist.get("31_recompression_ratios"): all_features.extend(recompression_ratios(row_np))
    if dic_checklist.get("32_haar_wavelet_detail_ratio"): all_features.append(haar_wavelet_detail_ratio(row_np))
    if dic_checklist.get("33_spectral_flatness_wiener"): all_features.append(spectral_flatness_measure(row_np))
    if dic_checklist.get("34_structural_run_and_ascii"): all_features.extend(structural_run_and_ascii(row_np))

    # --- AFRL Custom Suite (35..36) ---
    if dic_checklist.get("35_1_local_nibbles_variance_success") or dic_checklist.get("35_2_local_nibbles_variance_stat"):
        lnv_success, lnv_stat, _ = local_nibbles_variance(row_np)
        if dic_checklist.get("35_1_local_nibbles_variance_success"): all_features.append(1 if lnv_success else 0)
        if dic_checklist.get("35_2_local_nibbles_variance_stat"): all_features.append(lnv_stat)
    if dic_checklist.get("36_afrl_combined_success"):
        all_features.append(1 if AFRL_test(row_np) else 0)

    return all_features


def generate_feature_headers(dic_checklist):
    headers = []
    # NIST SP 800-22 (1..15)
    if dic_checklist.get('1_1_longest_run_ones_in_a_block_test_success'): headers.append('1_1_longest_run_ones_in_a_block_test_success')
    if dic_checklist.get('1_2_longest_run_ones_in_a_block_test_pvalue'): headers.append('1_2_longest_run_ones_in_a_block_test_pvalue')
    if dic_checklist.get('2_1_overlapping_template_matching_test_success'): headers.append('2_1_overlapping_template_matching_test_success')
    if dic_checklist.get('2_2_overlapping_template_matching_test_pvalue'): headers.append('2_2_overlapping_template_matching_test_pvalue')
    if dic_checklist.get('3_1_approximate_entropy_test_success'): headers.append('3_1_approximate_entropy_test_success')
    if dic_checklist.get('3_2_approximate_entropy_test_pvalue'): headers.append('3_2_approximate_entropy_test_pvalue')
    if dic_checklist.get('4_1_frequency_within_block_test_success'): headers.append('4_1_frequency_within_block_test_success')
    if dic_checklist.get('4_2_frequency_within_block_test_pvalue'): headers.append('4_2_frequency_within_block_test_pvalue')
    if dic_checklist.get('5_1_runs_test_success'): headers.append('5_1_runs_test_success')
    if dic_checklist.get('5_2_runs_test_pvalue'): headers.append('5_2_runs_test_pvalue')
    if dic_checklist.get('6_1_random_excursion_variant_test_success'): headers.append('6_1_random_excursion_variant_test_success')
    if dic_checklist.get('6_2_random_excursion_variant_test_pvalue'):
        for i in range(18): headers.append(f'6_2_random_excursion_variant_test_pvalue_{i}')
    if dic_checklist.get('8_1_cumulative_sums_test_success'): headers.append('8_1_cumulative_sums_test_success')
    if dic_checklist.get('8_2_cumulative_sums_test_pvalue'):
        for i in range(2): headers.append(f'8_2_cumulative_sums_test_pvalue_{i}')
    if dic_checklist.get('9_1_non_overlapping_template_matching_test_success'): headers.append('9_1_non_overlapping_template_matching_test_success')
    if dic_checklist.get('9_2_non_overlapping_template_matching_test_pvalue'): headers.append('9_2_non_overlapping_template_matching_test_pvalue')
    if dic_checklist.get('10_1_monobit_test_success'): headers.append('10_1_monobit_test_success')
    if dic_checklist.get('10_2_monobit_test_pvalue'): headers.append('10_2_monobit_test_pvalue')
    if dic_checklist.get('11_1_random_excursion_test_success'): headers.append('11_1_random_excursion_test_success')
    if dic_checklist.get('11_2_random_excursion_test_pvalue'):
        for i in range(8): headers.append(f'11_2_random_excursion_test_pvalue_{i}')
    if dic_checklist.get('12_1_dft_test_success'): headers.append('12_1_dft_test_success')
    if dic_checklist.get('12_2_dft_test_pvalue'): headers.append('12_2_dft_test_pvalue')
    if dic_checklist.get('13_1_serial_test_success'): headers.append('13_1_serial_test_success')
    if dic_checklist.get('13_2_serial_test_pvalue'):
        for i in range(2): headers.append(f'13_2_serial_test_pvalue_{i}')
    if dic_checklist.get('14_1_binary_matrix_rank_test_success'): headers.append('14_1_binary_matrix_rank_test_success')
    if dic_checklist.get('14_2_binary_matrix_rank_test_pvalue'): headers.append('14_2_binary_matrix_rank_test_pvalue')
    if dic_checklist.get('15_1_linear_complexity_test_success'): headers.append('15_1_linear_complexity_test_success')
    if dic_checklist.get('15_2_linear_complexity_test_pvalue'): headers.append('15_2_linear_complexity_test_pvalue')

    # HEDGE Chi-Square (16..18)
    if dic_checklist.get("16_chi_test_absolute_success"): headers.append('16_chi_test_absolute_success')
    if dic_checklist.get("17_chi_test_confidence_success"): headers.append('17_chi_test_confidence_success')
    if dic_checklist.get("18_1_chi2_uniform_stat"): headers.append('18_1_chi2_uniform_stat')
    if dic_checklist.get("18_2_chi2_uniform_pvalue"): headers.append('18_2_chi2_uniform_pvalue')

    # BLFCP & EnCoD (19..22)
    if dic_checklist.get("19_byte_frequency_distribution"):
        for i in range(256): headers.append(f'19_byte_frequency_distribution_{i}')
    if dic_checklist.get("20_discrete_fourier_transform"):
        for i in range(10): headers.append(f'20_discrete_fourier_transform_{i}')
    if dic_checklist.get("21_lempel_ziv_complexity"): headers.append('21_lempel_ziv_complexity')
    if dic_checklist.get("22_monte_carlo_pi_approximation"): headers.append('22_monte_carlo_pi_approximation')

    # SOTA 2024-2026 Multi-Modal, Markov & Spectral (23..34)
    if dic_checklist.get("23_byte_moments"):
        headers.extend(['23_byte_mean', '23_byte_variance', '23_byte_skewness', '23_byte_kurtosis'])
    if dic_checklist.get("24_ks_uniformity"):
        headers.extend(['24_ks_uniform_stat', '24_ks_uniform_pvalue'])
    if dic_checklist.get("25_wasserstein_distance_uniform"):
        headers.append('25_wasserstein_distance_uniform')
    if dic_checklist.get("26_index_of_coincidence"):
        headers.append('26_index_of_coincidence')
    if dic_checklist.get("27_generalized_entropies"):
        headers.extend(['27_renyi_collision_entropy', '27_min_entropy', '27_tsallis_entropy'])
    if dic_checklist.get("28_serial_and_lag_autocorr"):
        headers.extend(['28_serial_corr_lag1', '28_autocorr_lag2', '28_autocorr_lag4', '28_autocorr_lag8'])
    if dic_checklist.get("29_markov_bigram_metrics"):
        headers.extend(['29_bigram_joint_entropy', '29_conditional_entropy', '29_mutual_information', '29_unique_bigram_ratio'])
    if dic_checklist.get("30_byte_difference_stats"):
        headers.extend(['30_mod256_diff_entropy', '30_mean_abs_byte_jump'])
    if dic_checklist.get("31_recompression_ratios"):
        headers.extend(['31_recomp_ratio_zlib', '31_recomp_ratio_bz2', '31_recomp_ratio_lzma'])
    if dic_checklist.get("32_haar_wavelet_detail_ratio"):
        headers.append('32_haar_wavelet_detail_ratio')
    if dic_checklist.get("33_spectral_flatness_wiener"):
        headers.append('33_spectral_flatness_wiener')
    if dic_checklist.get("34_structural_run_and_ascii"):
        headers.extend(['34_max_byte_run', '34_printable_ascii_ratio'])

    # AFRL Custom Suite (35..36)
    if dic_checklist.get("35_1_local_nibbles_variance_success"):
        headers.append('35_1_local_nibbles_variance_success')
    if dic_checklist.get("35_2_local_nibbles_variance_stat"):
        headers.append('35_2_local_nibbles_variance_stat')
    if dic_checklist.get("36_afrl_combined_success"):
        headers.append('36_afrl_combined_success')

    return headers


def parallel_feature_extraction_large_file(window, input_file_path, output_file_path, dic_checklist, cancel_event, chunk_size=100):
    """Thread-safe worker function: communicates strictly via write_event_value."""
    try:
        total_cores = mp.cpu_count()
        num_cores = max(1, int(total_cores * dic_checklist["-cpu-"] / 100))

        window.write_event_value('-THREAD-UPDATE-', (0, 100, 'Stage 1/2: Counting fragments...', 'Calculating...'))
        with open(input_file_path, 'r', encoding='utf-8') as f:
            row_count = sum(1 for _ in f)
        total_chunks = max(1, ceil(row_count / chunk_size))
        if cancel_event.is_set():
            window.write_event_value('-THREAD-DONE-', (False, "Cancelled by user."))
            return

        window.write_event_value('-THREAD-UPDATE-', (0, total_chunks, f'Stage 2/2: Processing across {num_cores} CPU cores...', 'Starting...'))

        start_time = time.time()
        is_first_chunk = True
        header_list = generate_feature_headers(dic_checklist)
        process_row_fixed = partial(process_row, dic_checklist=dic_checklist)

        with mp.Pool(processes=num_cores) as pool:
            chunk_iterator = pd.read_csv(input_file_path, header=None, dtype=np.uint8, chunksize=chunk_size)
            for i, chunk_df in enumerate(chunk_iterator):
                if cancel_event.is_set():
                    window.write_event_value('-THREAD-DONE-', (False, "Process cancelled by user."))
                    return

                data_rows = list(chunk_df.to_numpy(dtype=np.uint8))
                results = pool.map(process_row_fixed, data_rows)

                features_df = pd.DataFrame(results)
                features_df.to_csv(
                    output_file_path, index=False,
                    mode='w' if is_first_chunk else 'a',
                    header=header_list if is_first_chunk else False
                )
                is_first_chunk = False

                chunk_written = i + 1
                elapsed_time = time.time() - start_time
                progress_percent = chunk_written / total_chunks if total_chunks > 0 else 0
                etr = (elapsed_time / progress_percent) - elapsed_time if progress_percent > 0 else 0

                status = f'Processed chunk {chunk_written} of {total_chunks} ({int(progress_percent * 100)}%)'
                time_str = f"Elapsed: {format_time(elapsed_time)} | ETR: {format_time(etr)}"
                window.write_event_value('-THREAD-UPDATE-', (chunk_written, total_chunks, status, time_str))

        window.write_event_value('-THREAD-DONE-', (True, f"Features successfully saved to: {output_file_path}"))
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
        frag_size = int(frag_size_str)
        frag_count = int(count_str)
        if frag_size <= 0 or frag_count <= 0:
            raise ValueError
    except ValueError:
        psg.popup_error("Fragment size and count must be positive integers.", title="Validation Error")
        return

    mode_titles = {
        "FOLDER_RANDOM": ("Dataset Random Fragment Extractor", f"Extracting {frag_count} total fragments ({frag_size} bytes each)"),
        "FILE_RANDOM": ("Per-File Fragment Extractor", f"Extracting {frag_count} fragments per file ({frag_size} bytes each)"),
        "FILE_PADDED": ("Padded Per-File Fragment Extractor", f"Extracting {frag_count} fragments per file with random padding ({frag_size} bytes)")
    }
    title, subtitle = mode_titles[mode]
    win_prog = create_progress_modal(title, subtitle)

    while True:
        ev, _ = win_prog.read()
        if ev in ("Exit", psg.WIN_CLOSED):
            break
        elif ev == "Start_HOVER_IN":
            win_prog["Start"].update(button_color=BTN_PRI_HOVER)
        elif ev == "Start_HOVER_OUT":
            win_prog["Start"].update(button_color=BTN_PRIMARY)
        elif ev == "Start":
            win_prog['-CNL-'].update(disabled=False)
            win_prog['Start'].update(disabled=True)
            if mode == "FOLDER_RANDOM":
                success = extract_whole_folder_fragments(win_prog, src_folder, frag_size, frag_count, dst_file)
            elif mode == "FILE_RANDOM":
                success = extract_random_fragments_file(win_prog, src_folder, frag_size, frag_count, dst_file)
            else:
                success = extract_random_fragments_file_random_pad(win_prog, src_folder, frag_size, frag_count, dst_file)

            if success:
                psg.popup("Extraction Complete!", f"Fragments saved to: {dst_file}", title="Success")
            else:
                psg.popup("Cancelled", "Fragment extraction was stopped.", title="Cancelled")
            break
    win_prog.close()


def open_feature_extractor_studio(input_csv_path="", output_csv_path=""):
    nistf_list_layout = [
        [psg.Checkbox("1.1 Longest Run of Ones (Success)", key="1_1_longest_run_ones_in_a_block_test_success", background_color=BG_CARD)],
        [psg.Checkbox("1.2 Longest Run of Ones (p-value)", key="1_2_longest_run_ones_in_a_block_test_pvalue", background_color=BG_CARD)],
        [psg.Checkbox("2.1 Overlapping Template (Success) [>1M bits]", key="2_1_overlapping_template_matching_test_success", disabled=True, background_color=BG_CARD, text_color=TEXT_MUTED)],
        [psg.Checkbox("2.2 Overlapping Template (p-value) [>1M bits]", key="2_2_overlapping_template_matching_test_pvalue", disabled=True, background_color=BG_CARD, text_color=TEXT_MUTED)],
        [psg.Checkbox("3.1 Approximate Entropy (Success)", key="3_1_approximate_entropy_test_success", background_color=BG_CARD)],
        [psg.Checkbox("3.2 Approximate Entropy (p-value)", key="3_2_approximate_entropy_test_pvalue", background_color=BG_CARD)],
        [psg.Checkbox("4.1 Frequency Within Block (Success)", key="4_1_frequency_within_block_test_success", background_color=BG_CARD)],
        [psg.Checkbox("4.2 Frequency Within Block (p-value)", key="4_2_frequency_within_block_test_pvalue", background_color=BG_CARD)],
        [psg.Checkbox("5.1 Runs Test (Success)", key="5_1_runs_test_success", background_color=BG_CARD)],
        [psg.Checkbox("5.2 Runs Test (p-value)", key="5_2_runs_test_pvalue", background_color=BG_CARD)],
        [psg.Checkbox("6.1 Random Excursion Variant (Success)", key="6_1_random_excursion_variant_test_success", background_color=BG_CARD)],
        [psg.Checkbox("6.2 Random Excursion Variant (18 p-values)", key="6_2_random_excursion_variant_test_pvalue", background_color=BG_CARD)],
        [psg.Checkbox("7.1 Maurer's Universal (Success) [>387K bits]", key="7_1_maurers_universal_test_success", disabled=True, background_color=BG_CARD, text_color=TEXT_MUTED)],
        [psg.Checkbox("7.2 Maurer's Universal (p-value) [>387K bits]", key="7_2_maurers_universal_test_pvalue", disabled=True, background_color=BG_CARD, text_color=TEXT_MUTED)],
        [psg.Checkbox("8.1 Cumulative Sums (Success)", key="8_1_cumulative_sums_test_success", background_color=BG_CARD)],
        [psg.Checkbox("8.2 Cumulative Sums (2 p-values)", key="8_2_cumulative_sums_test_pvalue", background_color=BG_CARD)],
        [psg.Checkbox("9.1 Non-Overlapping Template (Success)", key="9_1_non_overlapping_template_matching_test_success", background_color=BG_CARD)],
        [psg.Checkbox("9.2 Non-Overlapping Template (p-value)", key="9_2_non_overlapping_template_matching_test_pvalue", background_color=BG_CARD)],
        [psg.Checkbox("10.1 Monobit Frequency (Success)", key="10_1_monobit_test_success", background_color=BG_CARD)],
        [psg.Checkbox("10.2 Monobit Frequency (p-value)", key="10_2_monobit_test_pvalue", background_color=BG_CARD)],
        [psg.Checkbox("11.1 Random Excursion (Success)", key="11_1_random_excursion_test_success", background_color=BG_CARD)],
        [psg.Checkbox("11.2 Random Excursion (8 p-values)", key="11_2_random_excursion_test_pvalue", background_color=BG_CARD)],
        [psg.Checkbox("12.1 Discrete Fourier Transform (Success)", key="12_1_dft_test_success", background_color=BG_CARD)],
        [psg.Checkbox("12.2 Discrete Fourier Transform (p-value)", key="12_2_dft_test_pvalue", background_color=BG_CARD)],
        [psg.Checkbox("13.1 Serial Test (Success)", key="13_1_serial_test_success", background_color=BG_CARD)],
        [psg.Checkbox("13.2 Serial Test (2 p-values)", key="13_2_serial_test_pvalue", background_color=BG_CARD)],
        [psg.Checkbox("14.1 Binary Matrix Rank (Success)", key="14_1_binary_matrix_rank_test_success", background_color=BG_CARD)],
        [psg.Checkbox("14.2 Binary Matrix Rank (p-value)", key="14_2_binary_matrix_rank_test_pvalue", background_color=BG_CARD)],
        [psg.Checkbox("15.1 Linear Complexity (Success) [>1M bits]", key="15_1_linear_complexity_test_success", disabled=True, background_color=BG_CARD, text_color=TEXT_MUTED)],
        [psg.Checkbox("15.2 Linear Complexity (p-value) [>1M bits]", key="15_2_linear_complexity_test_pvalue", disabled=True, background_color=BG_CARD, text_color=TEXT_MUTED)],
    ]

    hedge_fs_layout = [
        [psg.Checkbox("16. Chi-Square Absolute Success", key="16_chi_test_absolute_success", background_color=BG_CARD)],
        [psg.Checkbox("17. Chi-Square Confidence Success", key="17_chi_test_confidence_success", background_color=BG_CARD)],
        [psg.Checkbox("18.1 Chi-Square Uniform Statistic", key="18_1_chi2_uniform_stat", background_color=BG_CARD)],
        [psg.Checkbox("18.2 Chi-Square Uniform p-value", key="18_2_chi2_uniform_pvalue", background_color=BG_CARD)],
    ]

    blfcp_layout = [
        [psg.Checkbox("19. Byte Frequency Distribution (256 cols)", key="19_byte_frequency_distribution", background_color=BG_CARD)],
        [psg.Checkbox("20. Discrete Fourier Transform (10 cols)", key="20_discrete_fourier_transform", background_color=BG_CARD)],
        [psg.Checkbox("21. Lempel-Ziv Complexity (LZ76)", key="21_lempel_ziv_complexity", background_color=BG_CARD)],
        [psg.Checkbox("22. Monte Carlo Pi Approximation", key="22_monte_carlo_pi_approximation", background_color=BG_CARD)],
    ]

    sota_layout = [
        [psg.Checkbox("23. Distributional Moments (Mean/Var/Skew/Kurt) [4]", key="23_byte_moments", background_color=BG_CARD)],
        [psg.Checkbox("24. Kolmogorov-Smirnov Uniformity (Stat & p-val) [2]", key="24_ks_uniformity", background_color=BG_CARD)],
        [psg.Checkbox("25. 1-Wasserstein (Earth Mover's) Distance to Uniform", key="25_wasserstein_distance_uniform", background_color=BG_CARD)],
        [psg.Checkbox("26. Index of Coincidence (IoC)", key="26_index_of_coincidence", background_color=BG_CARD)],
        [psg.Checkbox("27. Rényi Collision, Min & Tsallis Entropies [3]", key="27_generalized_entropies", background_color=BG_CARD)],
        [psg.Checkbox("28. Serial Correlation & Lag-{2,4,8} Autocorr [4]", key="28_serial_and_lag_autocorr", background_color=BG_CARD)],
        [psg.Checkbox("29. Markov Bigram & Mutual Info (ESORICS'26) [4]", key="29_markov_bigram_metrics", background_color=BG_CARD)],
        [psg.Checkbox("30. Byte Difference Entropy & Mean Jump [2]", key="30_byte_difference_stats", background_color=BG_CARD)],
        [psg.Checkbox("31. Multi-Algo Re-Compression (zlib/bz2/lzma) [3]", key="31_recompression_ratios", background_color=BG_CARD)],
        [psg.Checkbox("32. 1-Level Haar Wavelet Detail Energy Ratio", key="32_haar_wavelet_detail_ratio", background_color=BG_CARD)],
        [psg.Checkbox("33. Spectral Flatness Measure (Wiener Entropy)", key="33_spectral_flatness_wiener", background_color=BG_CARD)],
        [psg.Checkbox("34. Max Byte Run & Printable ASCII Ratio [2]", key="34_structural_run_and_ascii", background_color=BG_CARD)],
    ]

    afrl_layout = [
        [psg.Checkbox("35.1 Local Nibbles Variance (Success <= 46.98)", key="35_1_local_nibbles_variance_success", background_color=BG_CARD)],
        [psg.Checkbox("35.2 Local Nibbles Variance (Variance Stat)", key="35_2_local_nibbles_variance_stat", background_color=BG_CARD)],
        [psg.Checkbox("36. AFRL Combined Classifier (AppEn + Block + Runs + LNV)", key="36_afrl_combined_success", background_color=BG_CARD)],
    ]

    nist_frame = psg.Frame(
        ' NIST SP 800-22 Statistical Suite ',
        [[psg.Column(nistf_list_layout, scrollable=True, vertical_scroll_only=True, size=(375, 480), background_color=BG_CARD)]],
        background_color=BG_CARD, title_color=ACCENT_EMERALD, font=FONT_SECTION, pad=(6, 6), relief=psg.RELIEF_FLAT
    )

    hedge_frame = psg.Frame(
        ' HEDGE Chi-Square Suite ',
        [[psg.Column(hedge_fs_layout, background_color=BG_CARD)]],
        background_color=BG_CARD, title_color=ACCENT_EMERALD, font=FONT_SECTION, pad=(6, 6), expand_x=True, relief=psg.RELIEF_FLAT
    )

    blfcp_frame = psg.Frame(
        ' BLFCP & EnCoD Suite ',
        [[psg.Column(blfcp_layout, background_color=BG_CARD)]],
        background_color=BG_CARD, title_color=ACCENT_EMERALD, font=FONT_SECTION, pad=(6, 6), expand_x=True, relief=psg.RELIEF_FLAT
    )

    sota_frame = psg.Frame(
        ' SOTA Multi-Modal, Markov & Spectral Suite (2024–2026) ',
        [[psg.Column(sota_layout, background_color=BG_CARD)]],
        background_color=BG_CARD, title_color=ACCENT_EMERALD, font=FONT_SECTION, pad=(6, 6), expand_x=True, relief=psg.RELIEF_FLAT
    )

    afrl_frame = psg.Frame(
        ' AFRL Specialized Method (LNV + Ensemble) ',
        [[psg.Column(afrl_layout, background_color=BG_CARD)]],
        background_color=BG_CARD, title_color=ACCENT_EMERALD, font=FONT_SECTION, pad=(6, 6), expand_x=True, relief=psg.RELIEF_FLAT
    )

    mid_column = psg.Column([[hedge_frame], [blfcp_frame]], pad=(0, 0), expand_y=True)
    right_column = psg.Column([[sota_frame], [afrl_frame]], pad=(0, 0), expand_y=True)

    preset_bar = psg.Frame(
        ' Quick Selection & Benchmark Presets ',
        [[
            psg.Button("✓  Select All", key="-PRESET-ALL-", button_color=BTN_SECONDARY, font=FONT_BOLD, pad=(4, 6)),
            psg.Button("✕  Clear All", key="-PRESET-NONE-", button_color=BTN_SECONDARY, font=FONT_BOLD, pad=(4, 6)),
            psg.Text("│", text_color=BORDER_SUBTLE, background_color=BG_CARD, font=FONT_TITLE),
            psg.Button("★  AFRL", key="-PRESET-AFRL-", button_color=BTN_PRESET, font=FONT_BOLD, pad=(4, 6)),
            psg.Button("★  HEDGE", key="-PRESET-HEDGE-", button_color=BTN_PRESET, font=FONT_BOLD, pad=(4, 6)),
            psg.Button("★  BLFCP", key="-PRESET-BLFCP-", button_color=BTN_PRESET, font=FONT_BOLD, pad=(4, 6)),
            psg.Button("★  EnCoD", key="-PRESET-ENCOD-", button_color=BTN_PRESET, font=FONT_BOLD, pad=(4, 6)),
            psg.Button("★  SOTA '26", key="-PRESET-SOTA-", button_color=BTN_PRESET, font=FONT_BOLD, pad=(4, 6)),
            psg.Button("⚡  All NIST", key="-PRESET-NIST-", button_color=BTN_SECONDARY, font=FONT_BOLD, pad=(4, 6)),
        ]],
        background_color=BG_CARD, title_color=TEXT_MUTED, font=FONT_SUBTITLE, expand_x=True, pad=(6, 6), relief=psg.RELIEF_FLAT
    )

    io_bar = psg.Frame(
        ' Dataset Input / Output & Parallel Execution Settings ',
        [
            [psg.Text("Fragments CSV:", size=(15, 1), background_color=BG_CARD, font=FONT_BOLD),
             psg.Input(default_text=input_csv_path, key="-FEAT-IN-", readonly=True,
                       disabled_readonly_background_color=BG_INPUT, disabled_readonly_text_color=TEXT_PRIMARY, expand_x=True),
             psg.FileBrowse("📂  Browse CSV", key="-BRW-FEAT-IN-", file_types=(("CSV Files", "*.csv"),), button_color=BTN_SECONDARY, font=FONT_BOLD, pad=(6, 4))],
            [psg.Text("Output Features:", size=(15, 1), background_color=BG_CARD, font=FONT_BOLD),
             psg.Input(default_text=output_csv_path, key="-FEAT-OUT-", readonly=True,
                       disabled_readonly_background_color=BG_INPUT, disabled_readonly_text_color=TEXT_PRIMARY, expand_x=True),
             psg.FileSaveAs("💾  Save As CSV", key="-SAV-FEAT-OUT-", file_types=(("CSV Files", "*.csv"),), button_color=BTN_SECONDARY, font=FONT_BOLD, pad=(6, 4))],
            [psg.Text("CPU Allocation (%):", size=(15, 1), background_color=BG_CARD, font=FONT_BOLD),
             psg.Slider((10, 100), default_value=80, orientation='horizontal', key="-cpu-",
                        expand_x=True, background_color=BG_CARD, text_color=ACCENT_EMERALD),
             psg.Text(f"(System Cores: {mp.cpu_count()})", background_color=BG_CARD, text_color=TEXT_MUTED, font=FONT_SUBTITLE)]
        ],
        background_color=BG_CARD, title_color=ACCENT_EMERALD, font=FONT_SECTION, expand_x=True, pad=(6, 6), relief=psg.RELIEF_FLAT
    )

    layout = [
        [psg.Text("⚡ Statistical & Cryptographic Feature Extraction Studio", font=FONT_TITLE, text_color=ACCENT_EMERALD),
         psg.Push(),
         psg.Text("● Calibrated for 4096-byte fragments", font=FONT_BOLD, text_color=ACCENT_AMBER)],
        [preset_bar],
        [nist_frame, mid_column, right_column],
        [io_bar],
        [psg.Push(),
         psg.Button("✖  Cancel", key="-CANCEL-STUDIO-", button_color=BTN_SECONDARY, font=FONT_BOLD, size=(15, 1), pad=(8, 10)),
         psg.Button("🚀  Start Feature Extraction", key="-RUN-STUDIO-", button_color=BTN_PRIMARY, font=FONT_BTN_LG, size=(28, 1), pad=(8, 10)),
         psg.Push()]
    ]

    window_features = psg.Window("Feature Extraction Studio", layout, resizable=True, modal=True, finalize=True)
    set_window_icon(window_features)

    studio_hover_map = {
        "-PRESET-ALL-": (BTN_SECONDARY, BTN_SEC_HOVER),
        "-PRESET-NONE-": (BTN_SECONDARY, BTN_SEC_HOVER),
        "-PRESET-NIST-": (BTN_SECONDARY, BTN_SEC_HOVER),
        "-PRESET-AFRL-": (BTN_PRESET, BTN_PRE_HOVER),
        "-PRESET-HEDGE-": (BTN_PRESET, BTN_PRE_HOVER),
        "-PRESET-BLFCP-": (BTN_PRESET, BTN_PRE_HOVER),
        "-PRESET-ENCOD-": (BTN_PRESET, BTN_PRE_HOVER),
        "-PRESET-SOTA-": (BTN_PRESET, BTN_PRE_HOVER),
        "-CANCEL-STUDIO-": (BTN_SECONDARY, BTN_SEC_HOVER),
        "-RUN-STUDIO-": (BTN_PRIMARY, BTN_PRI_HOVER),
    }
    polish_window_widgets(window_features, studio_hover_map)

    def apply_selection(selected_set):
        for k in ALL_FEATURE_KEYS:
            if k not in DISABLED_NIST_KEYS:
                window_features[k].update(value=(k in selected_set))

    while True:
        event, values = window_features.read()
        if event in (psg.WIN_CLOSED, "-CANCEL-STUDIO-"):
            window_features.close()
            return

        if event.endswith('_HOVER_IN'):
            b_key = event.replace('_HOVER_IN', '')
            if b_key in studio_hover_map:
                window_features[b_key].update(button_color=studio_hover_map[b_key][1])
        elif event.endswith('_HOVER_OUT'):
            b_key = event.replace('_HOVER_OUT', '')
            if b_key in studio_hover_map:
                window_features[b_key].update(button_color=studio_hover_map[b_key][0])

        elif event == "-PRESET-ALL-":
            apply_selection(set(ALL_FEATURE_KEYS) - DISABLED_NIST_KEYS)
        elif event == "-PRESET-NONE-":
            apply_selection(set())
        elif event == "-PRESET-AFRL-":
            apply_selection(PRESET_AFRL)
        elif event == "-PRESET-HEDGE-":
            apply_selection(PRESET_HEDGE)
        elif event == "-PRESET-BLFCP-":
            apply_selection(PRESET_BLFCP)
        elif event == "-PRESET-ENCOD-":
            apply_selection(PRESET_ENCOD)
        elif event == "-PRESET-SOTA-":
            apply_selection(PRESET_SOTA)
        elif event == "-PRESET-NIST-":
            nist_keys = {k for k in ALL_FEATURE_KEYS if k.split('_')[0].isdigit() and int(k.split('_')[0]) <= 15}
            apply_selection(nist_keys - DISABLED_NIST_KEYS)

        elif event == "-RUN-STUDIO-":
            in_file = values.get("-FEAT-IN-", "").strip()
            out_file = values.get("-FEAT-OUT-", "").strip()
            selected_any = any(values.get(k, False) for k in ALL_FEATURE_KEYS)

            if not in_file or not os.path.isfile(in_file):
                psg.popup_error("Please select a valid input fragments CSV file.", title="Input Required")
                continue
            if not out_file:
                psg.popup_error("Please specify an output CSV file path.", title="Output Required")
                continue
            if not selected_any:
                psg.popup_error("Please select at least one feature or preset to extract.", title="No Features Selected")
                continue

            dic_checklist = values
            window_features.close()
            break

    # Launch Thread-Safe Progress Modal
    num_headers = len(generate_feature_headers(dic_checklist))
    window_progress = create_progress_modal(
        "Parallel Feature Extraction",
        f"Extracting {num_headers} feature columns from dataset..."
    )

    worker_thread, cancel_event = None, threading.Event()

    while True:
        event, prog_values = window_progress.read()
        if event in (psg.WIN_CLOSED, "Exit"):
            if worker_thread and worker_thread.is_alive():
                cancel_event.set()
            break
        elif event == "Start_HOVER_IN":
            window_progress["Start"].update(button_color=BTN_PRI_HOVER)
        elif event == "Start_HOVER_OUT":
            window_progress["Start"].update(button_color=BTN_PRIMARY)
        elif event == 'Start':
            window_progress['-CNL-'].update(disabled=False)
            window_progress['Start'].update(disabled=True)
            cancel_event.clear()
            worker_thread = threading.Thread(
                target=parallel_feature_extraction_large_file,
                args=(window_progress, in_file, out_file, dic_checklist, cancel_event),
                daemon=True
            )
            worker_thread.start()
        elif event == '-CNL-':
            window_progress['-STATUS-'].update("Stopping workers...")
            cancel_event.set()
        elif event == '-THREAD-UPDATE-':
            current_chunk, max_chunks, status, time_str = prog_values[event]
            window_progress['-PROGRESS-'].update(current_chunk, max=max_chunks)
            window_progress['-STATUS-'].update(status)
            window_progress['-TIME-'].update(time_str)
        elif event == '-THREAD-ERROR-':
            err_msg = prog_values[event]
            psg.popup_error(f"An error occurred during feature extraction: {err_msg}", title="Worker Error")
            break
        elif event == '-THREAD-DONE-':
            success, msg = prog_values[event]
            if success:
                psg.popup("Extraction Complete!", msg, title="Success")
            else:
                psg.popup("Cancelled", msg, title="Stopped")
            break

    window_progress.close()


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
        elif ev == "Start_HOVER_IN":
            win_prog["Start"].update(button_color=BTN_PRI_HOVER)
        elif ev == "Start_HOVER_OUT":
            win_prog["Start"].update(button_color=BTN_PRIMARY)
        elif ev == "Start":
            win_prog['-CNL-'].update(disabled=False)
            win_prog['Start'].update(disabled=True)
            if axis_mode == "ROWS":
                if 'concatenate_rows_gui' in globals():
                    success = concatenate_rows_gui(win_prog, [csv1, csv2], dst_csv, header=has_header)
                else:
                    psg.popup_error("Function 'concatenate_rows_gui' is missing in combine.py!", title="Missing Function")
                    success = False
            else:
                success = concatenate_columns_gui(win_prog, [csv1, csv2], dst_csv, header=has_header)

            if not success:
                psg.popup("Stopped", "CSV merge did not complete.", title="Stopped")
            break
    win_prog.close()


# =====================================================================
# MAIN DASHBOARD APPLICATION (WITH CUSTOM SEGMENTED PILL NAVIGATION)
# =====================================================================
def build_main_window():
    # Top App Header Bar
    header_banner = psg.Column([
        [psg.Text("🛡️ ENC FRAGMENT & FEATURE STUDIO", font=FONT_TITLE, text_color=ACCENT_EMERALD, background_color=BG_CARD),
         psg.Push(background_color=BG_CARD),
         psg.Button("📖  Guidance & Presets", key="-NAV-GUIDE-", button_color=BTN_SECONDARY, font=FONT_BOLD, pad=(6, 4)),
         psg.Button("ℹ️  About", key="-NAV-ABOUT-", button_color=BTN_SECONDARY, font=FONT_BOLD, pad=(6, 4))],
        [psg.Text("Cryptographic Fragment Slicer, Statistical Randomness Suite (NIST SP 800-22 / AFRL / HEDGE / BLFCP / SOTA '26) & Dataset Merger",
                  font=FONT_SUBTITLE, text_color=TEXT_MUTED, background_color=BG_CARD)]
    ], background_color=BG_CARD, pad=((0, 0), (0, 10)), expand_x=True)

    # Custom Segmented Pill Navigation Bar
    nav_pill_bar = psg.Column([
        [
            psg.Button("📂   1. Fragment Extractor", key="-TAB-BTN-1-", font=FONT_TAB,
                       button_color=TAB_ACTIVE_COLOR, size=(25, 2), pad=(4, 4)),
            psg.Button("⚡   2. Feature Studio", key="-TAB-BTN-2-", font=FONT_TAB,
                       button_color=TAB_INACTIVE_COLOR, size=(25, 2), pad=(4, 4)),
            psg.Button("🔗   3. Combine Datasets", key="-TAB-BTN-3-", font=FONT_TAB,
                       button_color=TAB_INACTIVE_COLOR, size=(25, 2), pad=(4, 4)),
        ]
    ], background_color=BG_CARD, pad=((0, 0), (0, 12)), expand_x=True, element_justification='center')

    # -----------------------------------------------------------------
    # VIEW 1: FRAGMENT EXTRACTOR
    # -----------------------------------------------------------------
    view_frag_layout = [
        [psg.Frame(' 1. Extraction Strategy ', [
            [psg.Radio("Dataset-Wide Random Sampling (Extract N total fragments across whole folder)",
                       "FRAG_MODE", key="-MODE-FOLDER-", default=True, enable_events=True, background_color=BG_CARD, font=FONT_BOLD)],
            [psg.Radio("Per-File Uniform Sampling (Extract M random fragments from each file)",
                       "FRAG_MODE", key="-MODE-FILE-", enable_events=True, background_color=BG_CARD, font=FONT_BOLD)],
            [psg.Radio("Per-File Sampling with Random Padding (Pad smaller files automatically)",
                       "FRAG_MODE", key="-MODE-PAD-", enable_events=True, background_color=BG_CARD, font=FONT_BOLD)],
        ], background_color=BG_CARD, title_color=ACCENT_EMERALD, font=FONT_SECTION, expand_x=True, pad=(6, 8), relief=psg.RELIEF_FLAT)],

        [psg.Frame(' 2. Parameters & Directories ', [
            [psg.Text("Source Folder:", size=(18, 1), background_color=BG_CARD, font=FONT_BOLD),
             psg.Input(key="-FRAG-SRC-", readonly=True, disabled_readonly_background_color=BG_INPUT,
                       disabled_readonly_text_color=TEXT_PRIMARY, expand_x=True),
             psg.FolderBrowse("📁  Choose Folder", key="-BRW-FRAG-SRC-", button_color=BTN_SECONDARY, font=FONT_BOLD, size=(16, 1), pad=(6, 4))],

            [psg.Text("Fragment Size (Bytes):", size=(18, 1), background_color=BG_CARD, font=FONT_BOLD),
             psg.Input("4096", key="-FRAG-SIZE-", size=(16, 1)),
             psg.Text("Recommended: 4096 bytes (Standard page size)", text_color=TEXT_MUTED, background_color=BG_CARD, font=FONT_SUBTITLE)],

            [psg.Text("Total Fragments (N):", key="-FRAG-COUNT-LBL-", size=(18, 1), background_color=BG_CARD, font=FONT_BOLD),
             psg.Input("1000", key="-FRAG-COUNT-", size=(16, 1)),
             psg.Text("Number of fragments to slice", text_color=TEXT_MUTED, background_color=BG_CARD, font=FONT_SUBTITLE)],

            [psg.Text("Destination CSV:", size=(18, 1), background_color=BG_CARD, font=FONT_BOLD),
             psg.Input(key="-FRAG-DST-", readonly=True, disabled_readonly_background_color=BG_INPUT,
                       disabled_readonly_text_color=TEXT_PRIMARY, expand_x=True),
             psg.FileSaveAs("💾  Save As CSV", key="-SAV-FRAG-DST-", file_types=(("CSV Files", "*.csv"),), button_color=BTN_SECONDARY, font=FONT_BOLD, size=(16, 1), pad=(6, 4))],
        ], background_color=BG_CARD, title_color=ACCENT_EMERALD, font=FONT_SECTION, expand_x=True, pad=(6, 8), relief=psg.RELIEF_FLAT)],

        [psg.VPush()],
        [psg.Push(),
         psg.Button("🚀   Extract Fragments Now", key="-BTN-RUN-FRAG-", button_color=BTN_PRIMARY, font=FONT_BTN_LG, size=(32, 2), pad=(8, 12)),
         psg.Push()],
        [psg.VPush()]
    ]

    # -----------------------------------------------------------------
    # VIEW 2: FEATURE EXTRACTOR
    # -----------------------------------------------------------------
    notice_text = chr(10).join([
        "All statistical thresholds (HEDGE Chi-Square bands, Binary Matrix Rank 28×28, AFRL LNV & Spectral suites)",
        "are calibrated and optimized for 4096-byte (32,768-bit) fragments."
    ])
    view_feat_layout = [
        [psg.Frame(' ⚠️ Benchmark Optimization Notice ', [
            [psg.Text(notice_text, text_color=ACCENT_AMBER, background_color=BG_CARD, font=FONT_BOLD, pad=(8, 6))]
        ], background_color=BG_CARD, title_color=ACCENT_AMBER, font=FONT_SECTION, expand_x=True, pad=(6, 8), relief=psg.RELIEF_FLAT)],

        [psg.Frame(' Quick Launch Configuration (Optional Pre-fill) ', [
            [psg.Text("Input Fragments CSV:", size=(18, 1), background_color=BG_CARD, font=FONT_BOLD),
             psg.Input(key="-QUICK-FEAT-IN-", readonly=True, disabled_readonly_background_color=BG_INPUT,
                       disabled_readonly_text_color=TEXT_PRIMARY, expand_x=True),
             psg.FileBrowse("📂  Browse CSV", key="-BRW-QFEAT-IN-", file_types=(("CSV Files", "*.csv"),), button_color=BTN_SECONDARY, font=FONT_BOLD, size=(16, 1), pad=(6, 4))],

            [psg.Text("Output Features CSV:", size=(18, 1), background_color=BG_CARD, font=FONT_BOLD),
             psg.Input(key="-QUICK-FEAT-OUT-", readonly=True, disabled_readonly_background_color=BG_INPUT,
                       disabled_readonly_text_color=TEXT_PRIMARY, expand_x=True),
             psg.FileSaveAs("💾  Save As CSV", key="-SAV-QFEAT-OUT-", file_types=(("CSV Files", "*.csv"),), button_color=BTN_SECONDARY, font=FONT_BOLD, size=(16, 1), pad=(6, 4))],
        ], background_color=BG_CARD, title_color=ACCENT_EMERALD, font=FONT_SECTION, expand_x=True, pad=(6, 8), relief=psg.RELIEF_FLAT)],

        [psg.VPush()],
        [psg.Push(),
         psg.Button("🎛️   Open Feature Selection Studio  (36 Suites & Presets)", key="-BTN4-",
                    button_color=BTN_PRIMARY, font=FONT_BTN_LG, size=(46, 2), pad=(8, 12)),
         psg.Push()],
        [psg.VPush()]
    ]

    # -----------------------------------------------------------------
    # VIEW 3: CSV DATASET COMBINER
    # -----------------------------------------------------------------
    view_combine_layout = [
        [psg.Frame(' 1. Merge Direction ', [
            [psg.Radio("Stack Vertically Along Rows (Axis 0 — Combine two fragment/feature datasets with identical columns)",
                       "CMB_MODE", key="-CMB-ROWS-", default=True, background_color=BG_CARD, font=FONT_BOLD)],
            [psg.Radio("Join Side-by-Side Along Columns (Axis 1 — Combine different feature sets for the same fragments)",
                       "CMB_MODE", key="-CMB-COLS-", background_color=BG_CARD, font=FONT_BOLD)],
        ], background_color=BG_CARD, title_color=ACCENT_EMERALD, font=FONT_SECTION, expand_x=True, pad=(6, 8), relief=psg.RELIEF_FLAT)],

        [psg.Frame(' 2. Source & Destination CSV Files ', [
            [psg.Text("First CSV File:", size=(16, 1), background_color=BG_CARD, font=FONT_BOLD),
             psg.Input(key="-CMB-CSV1-", readonly=True, disabled_readonly_background_color=BG_INPUT,
                       disabled_readonly_text_color=TEXT_PRIMARY, expand_x=True),
             psg.FileBrowse("📂  Browse CSV", key="-BRW-CMB-1-", file_types=(("CSV Files", "*.csv"),), button_color=BTN_SECONDARY, font=FONT_BOLD, size=(16, 1), pad=(6, 4))],

            [psg.Text("Second CSV File:", size=(16, 1), background_color=BG_CARD, font=FONT_BOLD),
             psg.Input(key="-CMB-CSV2-", readonly=True, disabled_readonly_background_color=BG_INPUT,
                       disabled_readonly_text_color=TEXT_PRIMARY, expand_x=True),
             psg.FileBrowse("📂  Browse CSV", key="-BRW-CMB-2-", file_types=(("CSV Files", "*.csv"),), button_color=BTN_SECONDARY, font=FONT_BOLD, size=(16, 1), pad=(6, 4))],

            [psg.Text("Merged Output:", size=(16, 1), background_color=BG_CARD, font=FONT_BOLD),
             psg.Input(key="-CMB-DST-", readonly=True, disabled_readonly_background_color=BG_INPUT,
                       disabled_readonly_text_color=TEXT_PRIMARY, expand_x=True),
             psg.FileSaveAs("💾  Save As CSV", key="-SAV-CMB-DST-", file_types=(("CSV Files", "*.csv"),), button_color=BTN_SECONDARY, font=FONT_BOLD, size=(16, 1), pad=(6, 4))],

            [psg.Checkbox("Input CSV files contain a header row", default=True, key="-CMB-HEADER-",
                          background_color=BG_CARD, font=FONT_BOLD, pad=((6, 0), (8, 4)))]
        ], background_color=BG_CARD, title_color=ACCENT_EMERALD, font=FONT_SECTION, expand_x=True, pad=(6, 8), relief=psg.RELIEF_FLAT)],

        [psg.VPush()],
        [psg.Push(),
         psg.Button("🔗   Combine CSV Datasets Now", key="-BTN-RUN-CMB-", button_color=BTN_PRIMARY, font=FONT_BTN_LG, size=(32, 2), pad=(8, 12)),
         psg.Push()],
        [psg.VPush()]
    ]

    # Container panels for custom tab switching
    col_view_1 = psg.Column(view_frag_layout, key="-VIEW-1-", visible=True, expand_x=True, expand_y=True, pad=(0, 0))
    col_view_2 = psg.Column(view_feat_layout, key="-VIEW-2-", visible=False, expand_x=True, expand_y=True, pad=(0, 0))
    col_view_3 = psg.Column(view_combine_layout, key="-VIEW-3-", visible=False, expand_x=True, expand_y=True, pad=(0, 0))

    status_footer = psg.Column([
        [psg.Text(f"● System Ready   │   Detected {mp.cpu_count()} Logical CPU Cores   │   36 Feature Suites Loaded (NIST + AFRL + SOTA)",
                  font=FONT_SUBTITLE, text_color=TEXT_MUTED, background_color=BG_CARD),
         psg.Push(background_color=BG_CARD),
         psg.Text("Studio v0.9.5", font=FONT_SUBTITLE, text_color=ACCENT_EMERALD, background_color=BG_CARD)]
    ], background_color=BG_CARD, pad=((0, 0), (10, 0)), expand_x=True)

    main_layout = [
        [header_banner],
        [nav_pill_bar],
        [col_view_1, col_view_2, col_view_3],
        [status_footer]
    ]

    window = psg.Window(
        'Enc Fragment Extractor — Studio Edition',
        main_layout,
        size=(880, 660),
        resizable=True,
        finalize=True
    )
    set_window_icon(window)

    main_hover_map = {
        "-NAV-GUIDE-": (BTN_SECONDARY, BTN_SEC_HOVER),
        "-NAV-ABOUT-": (BTN_SECONDARY, BTN_SEC_HOVER),
        "-BTN-RUN-FRAG-": (BTN_PRIMARY, BTN_PRI_HOVER),
        "-BTN4-": (BTN_PRIMARY, BTN_PRI_HOVER),
        "-BTN-RUN-CMB-": (BTN_PRIMARY, BTN_PRI_HOVER),
        "-TAB-BTN-1-": (TAB_ACTIVE_COLOR, TAB_HOVER_COLOR),
        "-TAB-BTN-2-": (TAB_INACTIVE_COLOR, TAB_HOVER_COLOR),
        "-TAB-BTN-3-": (TAB_INACTIVE_COLOR, TAB_HOVER_COLOR),
    }
    polish_window_widgets(window, main_hover_map)

    return window, main_hover_map


if __name__ == '__main__':
    mp.freeze_support()
    mp.set_start_method('spawn', force=True)

    window, main_hover_map = build_main_window()
    active_tab = 1

    def switch_tab(target_idx):
        global active_tab
        active_tab = target_idx
        for idx in (1, 2, 3):
            is_active = (idx == target_idx)
            window[f"-VIEW-{idx}-"].update(visible=is_active)
            window[f"-TAB-BTN-{idx}-"].update(
                button_color=TAB_ACTIVE_COLOR if is_active else TAB_INACTIVE_COLOR
            )

    while True:
        event, values = window.read()
        if event in ("Exit", psg.WIN_CLOSED):
            break

        # Custom Segmented Pill Tab Switching
        if event == "-TAB-BTN-1-":
            switch_tab(1)
        elif event == "-TAB-BTN-2-":
            switch_tab(2)
        elif event == "-TAB-BTN-3-":
            switch_tab(3)

        # Top Bar Modals
        elif event == "-NAV-ABOUT-":
            show_about_modal()
        elif event == "-NAV-GUIDE-":
            show_guidance_modal()

        # Dynamic Radio Label Update in Tab 1
        elif event == "-MODE-FOLDER-":
            window["-FRAG-COUNT-LBL-"].update("Total Fragments (N):")
        elif event in ("-MODE-FILE-", "-MODE-PAD-"):
            window["-FRAG-COUNT-LBL-"].update("Fragments / File (M):")

        # Smooth Button & Tab Hover Effects
        elif event.endswith('_HOVER_IN'):
            b_key = event.replace('_HOVER_IN', '')
            if b_key.startswith("-TAB-BTN-"):
                tab_num = int(b_key.split("-")[-2])
                if tab_num != active_tab:
                    window[b_key].update(button_color=TAB_HOVER_COLOR)
            elif b_key in main_hover_map:
                window[b_key].update(button_color=main_hover_map[b_key][1])

        elif event.endswith('_HOVER_OUT'):
            b_key = event.replace('_HOVER_OUT', '')
            if b_key.startswith("-TAB-BTN-"):
                tab_num = int(b_key.split("-")[-2])
                window[b_key].update(
                    button_color=TAB_ACTIVE_COLOR if tab_num == active_tab else TAB_INACTIVE_COLOR
                )
            elif b_key in main_hover_map:
                window[b_key].update(button_color=main_hover_map[b_key][0])

        # View 1: Execute Fragment Extraction
        elif event == '-BTN-RUN-FRAG-':
            if values.get("-MODE-FOLDER-"):
                mode = "FOLDER_RANDOM"
            elif values.get("-MODE-FILE-"):
                mode = "FILE_RANDOM"
            else:
                mode = "FILE_PADDED"
            run_fragment_extraction_workflow(
                mode,
                values.get("-FRAG-SRC-", "").strip(),
                values.get("-FRAG-SIZE-", "4096").strip(),
                values.get("-FRAG-COUNT-", "1000").strip(),
                values.get("-FRAG-DST-", "").strip()
            )

        # View 2: Open Feature Extraction Studio
        elif event == '-BTN4-':
            open_feature_extractor_studio(
                input_csv_path=values.get("-QUICK-FEAT-IN-", "").strip(),
                output_csv_path=values.get("-QUICK-FEAT-OUT-", "").strip()
            )

        # View 3: Execute CSV Merge
        elif event == '-BTN-RUN-CMB-':
            axis_mode = "ROWS" if values.get("-CMB-ROWS-") else "COLS"
            run_csv_combine_workflow(
                axis_mode,
                values.get("-CMB-CSV1-", "").strip(),
                values.get("-CMB-CSV2-", "").strip(),
                values.get("-CMB-HEADER-", True),
                values.get("-CMB-DST-", "").strip()
            )

    window.close()