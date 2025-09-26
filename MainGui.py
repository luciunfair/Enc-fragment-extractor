import FreeSimpleGUI as psg
import sys
import os
import numpy as np
import pandas as pd
import multiprocessing as mp
import time
import threading  # <-- Only necessary import added
from functools import partial
from math import ceil
import base64
import tkinter as tk
from PIL import Image, ImageTk


# This function determines the correct base path whether running as a script or as a frozen exe.
def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")

    return os.path.join(base_path, relative_path)

# Use the function to correctly set your system paths
sys.path.append(resource_path("fragment_extractor"))
sys.path.append(resource_path("feature_extractor"))
sys.path.append(resource_path("feature_extractor/NIST/"))
sys.path.append(resource_path("feature_extractor/HEDGE_specials/"))
sys.path.append(resource_path("feature_extractor/BLFCP_specials/"))
sys.path.append(resource_path("combine_csv/"))
# --- END of PATH FIX ---



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
from combine import *

# Your original theme and settings, untouched
psg.LOOK_AND_FEEL_TABLE['DarkGreen'] = {
    'BACKGROUND': '#2E2E2E', 'TEXT': '#FFFFFF', 'INPUT': '#3C3C3C',
    'TEXT_INPUT': '#FFFFFF', 'SCROLL': '#50C878', 'BUTTON': ('#FFFFFF', '#4CAF50'),
    'PROGRESS': psg.DEFAULT_PROGRESS_BAR_COLOR, 'BORDER': 1, 'SLIDER_DEPTH': 0, 'PROGRESS_DEPTH': 0,
}

# Set the default font for the entire application
psg.set_options(font=('Segoe UI', 10))

NORMAL_COLOR = ('#FFFFFF', '#4CAF50')
HOVER_COLOR = ('#FFFFFF', '#50C878')
readonly_bgcolor = '#252525'
readonly_textcolor = '#A0A0A0'
psg.theme('DarkGreen')


def guidance():
    psg.popup(
        "first extract fragments from folder of files you want and save it to a csv file, 4096 bytes fragmets recommended as it is the most common one",
        "then use feature extracten from extracted fragments",
        "features that end with the word 'success' are binary: 0 means nonencrypted, 1 means encrypted", " ",
        "HEDGE full features include: ",
        "   -3_1_approximate_entropy_test_success",
        "   -4_1_frequency_within_block_test_success",
        "   -8_1_cumulative_sums_test_success",
        "   -16_chi_test_absolute_success",
        "   -17_chi_test_confidence_success", " ",
        "EnCoD full features include: ",
        "   -19_byte_frequency_distribution", " ",
        "BLFCP full features include: ",
        "   -18_1_chi2_uniform_stat",
        "   -18_2_chi2_uniform_pvalue",
        "   -19_byte_frequency_distribution",
        "   -20_discrete_fourier_transform",
        "   -21_lempel_ziv_complexity",
        "   -22_monte_carlo_pi_approximation", " ",
        
        button_justification="center"
    )

def button_1():
    layout = [
        [psg.VPush()],
        [psg.Text("source folder", tooltip="the folder that you want to exctract fragments from"), psg.Input(key="-INP1B1-", readonly=True, disabled_readonly_background_color=readonly_bgcolor, disabled_readonly_text_color=readonly_textcolor, expand_x=True),psg.FolderBrowse("choose folder")],
        [psg.Text("fragment size", tooltip="how many bytes each fragment have e.g. 4096 means 4096 bytes"), psg.Input(key="-INP2B1-", expand_x=True)],
        [[psg.Text("total fragments", tooltip="it means how many total fragments randomly will extract from all files"), psg.Input(key="-INP3B1-", expand_x=True)]],
        [psg.Text("destination file folder", tooltip="the file name and path that you want to save exctracted fragments to"), psg.Input(key="-INP4B1-", readonly=True, disabled_readonly_background_color=readonly_bgcolor, disabled_readonly_text_color=readonly_textcolor, expand_x=True), psg.FileSaveAs(file_types=((('CSV Files', '*.csv'),)))],
        [psg.Column([[psg.Button("extract", key="-ETC-")]], expand_x=True, element_justification='center' )],
        [psg.VPush()],
    ]
    window = psg.Window("extract N fragments from whole folder randomly", layout, resizable=True)
    while True:
        event, values = window.read()
        if event in ["Exit", psg.WIN_CLOSED]: break
        if event=="-ETC-":
            layout_2 = [
                [psg.Text("This app will extract random fragments from files.")],
                [psg.Text("Status:"), psg.Text("Idle", key='-STATUS-')],
                [psg.ProgressBar(1, orientation='h', size=(50, 20), key='-PROGRESS-')],
                [psg.Text("", key='-TIME-')],
                [psg.Button("Start"), psg.Button("Cancel", key="-CNL-", disabled=True)]
            ]
            window_2 = psg.Window("Fragment Extractor", layout_2)
            while True:
                event_2, values_2 = window_2.read()
                if event_2 in ["Exit", psg.WIN_CLOSED]: break
                if event_2 == 'Start':
                    window_2['-CNL-'].update(disabled=False)
                    window_2['Start'].update(disabled=True)
                    success = extract_whole_folder_fragments(window_2, values["-INP1B1-"], int(values["-INP2B1-"]), int(values["-INP3B1-"]), values["-INP4B1-"])
                    if success: psg.popup("Done!", "The process completed successfully."); break
                    else: psg.popup("Cancelled", "The process was cancelled by the user."); break
                    window_2['-CNL-'].update(disabled=True)
                    window_2['Start'].update(disabled=False)
            window_2.close()
            window.close()
    window.close()

def button_2():
    layout = [
        [psg.VPush()],
        [psg.Text("source folder", tooltip="the folder that you want to exctract fragments from"), psg.Input(key="-INP1B1-", readonly=True, disabled_readonly_background_color=readonly_bgcolor, disabled_readonly_text_color=readonly_textcolor, expand_x=True),psg.FolderBrowse("choose folder")],
        [psg.Text("fragment size", tooltip="how many bytes each fragment have e.g. 4096 means 4096 bytes"), psg.Input(key="-INP2B1-", expand_x=True)],
        [[psg.Text("each file fragment", tooltip="it means how many  fragments randomly will extract from each files"), psg.Input(key="-INP3B1-", expand_x=True)]],
        [psg.Text("destination file folder", tooltip="the file name and path that you want to save exctracted fragments to"), psg.Input(key="-INP4B1-", readonly=True, disabled_readonly_background_color=readonly_bgcolor, disabled_readonly_text_color=readonly_textcolor, expand_x=True), psg.FileSaveAs(file_types=((('CSV Files', '*.csv'),)))],
        [psg.Column([[psg.Button("extract", key="-ETC-")]], expand_x=True, element_justification='center' )],
        [psg.VPush()],
    ]
    window = psg.Window("extract N fragments from whole folder randomly", layout, resizable=True)
    while True:
        event, values = window.read()
        if event in ["Exit", psg.WIN_CLOSED]: break
        if event=="-ETC-":
            layout_2 = [
                [psg.Text("This app will extract random fragments from files.")],
                [psg.Text("Status:"), psg.Text("Idle", key='-STATUS-')],
                [psg.ProgressBar(1, orientation='h', size=(50, 20), key='-PROGRESS-')],
                [psg.Text("", key='-TIME-')],
                [psg.Button("Start"), psg.Button("Cancel", key="-CNL-", disabled=True)]
            ]
            window_2 = psg.Window("Fragment Extractor", layout_2)
            while True:
                event_2, values_2 = window_2.read()
                if event_2 in ["Exit", psg.WIN_CLOSED]: break
                if event_2 == 'Start':
                    window_2['-CNL-'].update(disabled=False)
                    window_2['Start'].update(disabled=True)
                    success = extract_random_fragments_file(window_2, values["-INP1B1-"], int(values["-INP2B1-"]), int(values["-INP3B1-"]), values["-INP4B1-"])
                    if success: psg.popup("Done!", "The process completed successfully."); break
                    else: psg.popup("Cancelled", "The process was cancelled by the user."); break
                    window_2['-CNL-'].update(disabled=True)
                    window_2['Start'].update(disabled=False)
            window_2.close()
            window.close()
    window.close()

def button_3():
    layout = [
        [psg.VPush()],
        [psg.Text("source folder", tooltip="the folder that you want to exctract fragments from"), psg.Input(key="-INP1B1-", readonly=True, disabled_readonly_background_color=readonly_bgcolor, disabled_readonly_text_color=readonly_textcolor, expand_x=True),psg.FolderBrowse("choose folder")],
        [psg.Text("fragment size", tooltip="how many bytes each fragment have e.g. 4096 means 4096 bytes"), psg.Input(key="-INP2B1-", expand_x=True)],
        [[psg.Text("each file fragment", tooltip="it means how many  fragments randomly will extract from each files"), psg.Input(key="-INP3B1-", expand_x=True)]],
        [psg.Text("destination file folder", tooltip="the file name and path that you want to save exctracted fragments to"), psg.Input(key="-INP4B1-", readonly=True, disabled_readonly_background_color=readonly_bgcolor, disabled_readonly_text_color=readonly_textcolor, expand_x=True), psg.FileSaveAs(file_types=((('CSV Files', '*.csv'),)))],
        [psg.Column([[psg.Button("extract", key="-ETC-")]], expand_x=True, element_justification='center' )],
        [psg.VPush()],
    ]
    window = psg.Window("extract N fragments from whole folder randomly", layout, resizable=True)
    while True:
        event, values = window.read()
        if event in ["Exit", psg.WIN_CLOSED]: break
        if event=="-ETC-":
            layout_2 = [
                [psg.Text("This app will extract random fragments from files.")],
                [psg.Text("Status:"), psg.Text("Idle", key='-STATUS-')],
                [psg.ProgressBar(1, orientation='h', size=(50, 20), key='-PROGRESS-')],
                [psg.Text("", key='-TIME-')],
                [psg.Button("Start"), psg.Button("Cancel", key="-CNL-", disabled=True)]
            ]
            window_2 = psg.Window("Fragment Extractor", layout_2)
            while True:
                event_2, values_2 = window_2.read()
                if event_2 in ["Exit", psg.WIN_CLOSED]: break
                if event_2 == 'Start':
                    window_2['-CNL-'].update(disabled=False)
                    window_2['Start'].update(disabled=True)
                    success = extract_random_fragments_file_random_pad(window_2, values["-INP1B1-"], int(values["-INP2B1-"]), int(values["-INP3B1-"]), values["-INP4B1-"])
                    if success: psg.popup("Done!", "The process completed successfully."); break
                    else: psg.popup("Cancelled", "The process was cancelled by the user."); break
                    window_2['-CNL-'].update(disabled=True)
                    window_2['Start'].update(disabled=False)
            window_2.close()
            window.close()
    window.close()

# Your helper functions, untouched
def byte_to_bit(row):
    bit_list = [format(num, '08b') for num in row]
    bit_s_list = []
    for bit_8 in bit_list:
        for bit in bit_8:
            bit_s_list.append(int(bit))
    return bit_s_list

def format_time(seconds):
    if seconds is None or seconds < 0: return "Calculating..."
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

# =====================================================================
# MODIFIED SECTION: button_4 and its worker function are now threaded
# =====================================================================
def button_4():
    file_path = psg.popup_get_file(" the exctracted fragments csv file :", file_types=(("CSV Files", "*.csv"),))
    if not file_path: return

    nistf_list_layout = [
        [psg.Checkbox("1_1_longest_run_ones_in_a_block_test_success", key="1_1_longest_run_ones_in_a_block_test_success"),],
        [psg.Checkbox("1_2_longest_run_ones_in_a_block_test_pvalue", key="1_2_longest_run_ones_in_a_block_test_pvalue")],
        [psg.Checkbox("2_1_overlapping_template_matching_test_success", key="2_1_overlapping_template_matching_test_success", disabled=True)],
        [psg.Checkbox("2_2_overlapping_template_matching_test_pvalue", key="2_2_overlapping_template_matching_test_pvalue", disabled=True)],
        [psg.Checkbox("3_1_approximate_entropy_test_success", key="3_1_approximate_entropy_test_success")],
        [psg.Checkbox("3_2_approximate_entropy_test_pvalue", key="3_2_approximate_entropy_test_pvalue")],
        [psg.Checkbox("4_1_frequency_within_block_test_success", key="4_1_frequency_within_block_test_success")],
        [psg.Checkbox("4_2_frequency_within_block_test_pvalue", key="4_2_frequency_within_block_test_pvalue")],
        [psg.Checkbox("5_1_runs_test_success", key="5_1_runs_test_success")],
        [psg.Checkbox("5_2_runs_test_pvalue", key="5_2_runs_test_pvalue")],
        [psg.Checkbox("6_1_random_excursion_variant_test_success", key="6_1_random_excursion_variant_test_success")],
        [psg.Checkbox("6_2_random_excursion_variant_test_pvalue", key="6_2_random_excursion_variant_test_pvalue"), psg.Text("(18 features)")],
        [psg.Checkbox("7_1_maurers_universal_test_success", key="7_1_maurers_universal_test_success", disabled=True)],
        [psg.Checkbox("7_2_maurers_universal_test_pvalue", key="7_2_maurers_universal_test_pvalue", disabled=True)],
        [psg.Checkbox("8_1_cumulative_sums_test_success", key="8_1_cumulative_sums_test_success")],
        [psg.Checkbox("8_2_cumulative_sums_test_pvalue", key="8_2_cumulative_sums_test_pvalue"), psg.Text("(2 features)")],
        [psg.Checkbox("9_1_non_overlapping_template_matching_test_success", key="9_1_non_overlapping_template_matching_test_success")],
        [psg.Checkbox("9_2_non_overlapping_template_matching_test_pvalue", key="9_2_non_overlapping_template_matching_test_pvalue")],
        [psg.Checkbox("10_1_monobit_test_success", key="10_1_monobit_test_success")],
        [psg.Checkbox("10_2_monobit_test_pvalue", key="10_2_monobit_test_pvalue")],
        [psg.Checkbox("11_1_random_excursion_test_success", key="11_1_random_excursion_test_success")],
        [psg.Checkbox("11_2_random_excursion_test_pvalue", key="11_2_random_excursion_test_pvalue"), psg.Text("(8 features)")],
        [psg.Checkbox("12_1_dft_test_success", key="12_1_dft_test_success")],
        [psg.Checkbox("12_2_dft_test_pvalue", key="12_2_dft_test_pvalue")],
        [psg.Checkbox("13_1_serial_test_success", key="13_1_serial_test_success")],
        [psg.Checkbox("13_2_serial_test_pvalue", key="13_2_serial_test_pvalue"), psg.Text("(2 features)")],
        [psg.Checkbox("14_1_binary_matrix_rank_test_success", key="14_1_binary_matrix_rank_test_success")],
        [psg.Checkbox("14_2_binary_matrix_rank_test_pvalue", key="14_2_binary_matrix_rank_test_pvalue")],
        [psg.Checkbox("15_1_linear_complexity_test_success", key="15_1_linear_complexity_test_success", disabled=True)],
        [psg.Checkbox("15_2_linear_complexity_test_pvalue", key="15_2_linear_complexity_test_pvalue", disabled=True)],
    ]
    
    hedge_fs_layout = [
        [psg.Checkbox("16_chi_test_absolute_success", key="16_chi_test_absolute_success")],
        [psg.Checkbox("17_chi_test_confidence_success", key="17_chi_test_confidence_success")],
        [psg.Checkbox("18_1_chi2_uniform_stat", key="18_1_chi2_uniform_stat")],
        [psg.Checkbox("18_2_chi2_uniform_pvalue", key="18_2_chi2_uniform_pvalue")],
    ]

    # Define the two frames first
    nist_frame = psg.Frame('NIST Features',
        [[psg.Column(nistf_list_layout, scrollable=True, vertical_scroll_only=True, size=(450, 400),
                    element_justification='left')]]
    )

    hedge_frame = psg.Frame('HEDGE special Features',
        [[psg.Column(hedge_fs_layout, element_justification='left')]]
    )

    blfcp_layout = [
        [psg.Checkbox("19_byte_frequency_distribution", key="19_byte_frequency_distribution"), psg.Text("(256 features)")],
        [psg.Checkbox("20_discrete_fourier_transform", key="20_discrete_fourier_transform"), psg.Text("(10 features)")],
        [psg.Checkbox("21_lempel_ziv_complexity", key="21_lempel_ziv_complexity")],
        [psg.Checkbox("22_monte_carlo_pi_approximation", key="22_monte_carlo_pi_approximation")],
    ]
    
    blfcp_frame = psg.Frame('BLFCP special Features',
        [[psg.Column(blfcp_layout, element_justification='left')]]
    )
    
    mid_column_layout = [[hedge_frame],[blfcp_frame]]
    mid_column = psg.Column(mid_column_layout)
    # Arrange the frames in the final layout
    layout = [
        [psg.Push(), psg.Text("choose features you want to extract: "), psg.Push()],
        [nist_frame, psg.Push(), mid_column, psg.Push()],
        [psg.Text("cpu usage: ", pad=((0,0),(10,0))), psg.Slider((10,100), default_value=80, orientation='horizontal', key="-cpu-",expand_x=True)],
        [psg.Push(), psg.Ok(size=(4,1)), psg.Push()]
    ]
    window_features = psg.Window("feature extraction", layout, resizable=True)
    event, values = window_features.read()
    window_features.close()
    if event != "Ok": return

    dic_checklist = values
    save_path = psg.popup_get_file(" save csv file :", file_types=(("CSV Files", "*.csv"),), save_as=True)
    if not save_path: return

    layout_progress = [
        [psg.Text("This app will extract faetures from fragments.")],
        [psg.Text("Status:"), psg.Text("Idle", key='-STATUS-')],
        [psg.ProgressBar(1, orientation='h', size=(50, 20), key='-PROGRESS-')],
        [psg.Text("", key='-TIME-')],
        [psg.Button("Start"), psg.Button("Cancel", key="-CNL-", disabled=True)]
    ]
    window_progress = psg.Window("Feature Extractor", layout_progress, finalize=True)
    
    worker_thread, cancel_event = None, threading.Event()

    while True:
        event, values = window_progress.read()
        if event in (psg.WIN_CLOSED, "Exit"):
            if worker_thread and worker_thread.is_alive(): cancel_event.set()
            break
        if event == 'Start':
            window_progress['-CNL-'].update(disabled=False)
            window_progress['Start'].update(disabled=True)
            cancel_event.clear()
            worker_thread = threading.Thread(
                target=parallel_feature_extraction_large_file,
                args=(window_progress, file_path, save_path, dic_checklist, cancel_event),
                daemon=True)
            worker_thread.start()
        elif event == '-CNL-':
            cancel_event.set()
        elif event == '-THREAD-UPDATE-':
            progress, status, time_str = values[event]
            window_progress['-PROGRESS-'].update(progress)
            window_progress['-STATUS-'].update(status)
            window_progress['-TIME-'].update(time_str)
        elif event == '-THREAD-DONE-':
            success = values[event]
            psg.popup("Done!" if success else "Cancelled or Failed", "The process completed successfully." if success else "The process was stopped or an error occurred.")
            break
    window_progress.close()


def process_row(row, dic_checklist):
    bitn_list = ['1_1_longest_run_ones_in_a_block_test_success', '1_2_longest_run_ones_in_a_block_test_pvalue', '2_1_overlapping_template_matching_test_success', '2_2_overlapping_template_matching_test_pvalue', '3_1_approximate_entropy_test_success', '3_2_approximate_entropy_test_pvalue', '4_1_frequency_within_block_test_success', '4_2_frequency_within_block_test_pvalue', '5_1_runs_test_success', '5_2_runs_test_pvalue', '6_1_random_excursion_variant_test_success', '6_2_random_excursion_variant_test_pvalue', '7_1_maurers_universal_test_success', '7_2_maurers_universal_test_pvalue', '8_1_cumulative_sums_test_success', '8_2_cumulative_sums_test_pvalue', '9_1_non_overlapping_template_matching_test_success', '9_2_non_overlapping_template_matching_test_pvalue', '10_1_monobit_test_success', '10_2_monobit_test_pvalue', '11_1_random_excursion_test_success', '11_2_random_excursion_test_pvalue', '12_1_dft_test_success', '12_2_dft_test_pvalue', '13_1_serial_test_success', '13_2_serial_test_pvalue', '14_1_binary_matrix_rank_test_success', '14_2_binary_matrix_rank_test_pvalue', '15_1_linear_complexity_test_success', '15_2_linear_complexity_test_pvalue']
    bitn_values = [dic_checklist[k] for k in bitn_list]
    if any(bitn_values):
        bits = byte_to_bit(row)
    all_features = []
    if dic_checklist['1_1_longest_run_ones_in_a_block_test_success']==True or dic_checklist['1_2_longest_run_ones_in_a_block_test_pvalue']==True:
        success, pval, _ = longest_run_ones_in_a_block_test(bits)
        if dic_checklist['1_1_longest_run_ones_in_a_block_test_success']==True: all_features.append(1 if success==True else 0)
        if dic_checklist['1_2_longest_run_ones_in_a_block_test_pvalue']==True: all_features.append(pval)
    if dic_checklist['2_1_overlapping_template_matching_test_success']==True or dic_checklist['2_2_overlapping_template_matching_test_pvalue']==True:
        success, pval, _ = overlapping_template_matching_test(bits)
        if dic_checklist['2_1_overlapping_template_matching_test_success']==True: all_features.append(1 if success==True else 0)
        if dic_checklist['2_2_overlapping_template_matching_test_pvalue']==True: all_features.append(pval)
    if dic_checklist['3_1_approximate_entropy_test_success']==True or dic_checklist['3_2_approximate_entropy_test_pvalue']==True:
        success, pval, _ = approximate_entropy_test(bits)
        if dic_checklist['3_1_approximate_entropy_test_success']==True: all_features.append(1 if success==True else 0)
        if dic_checklist['3_2_approximate_entropy_test_pvalue']==True: all_features.append(pval)
    if dic_checklist['4_1_frequency_within_block_test_success']==True or dic_checklist['4_2_frequency_within_block_test_pvalue']==True:
        success, pval, _ = frequency_within_block_test(bits)
        if dic_checklist['4_1_frequency_within_block_test_success']==True: all_features.append(1 if success==True else 0)
        if dic_checklist['4_2_frequency_within_block_test_pvalue']==True: all_features.append(pval)
    if dic_checklist['5_1_runs_test_success']==True or dic_checklist['5_2_runs_test_pvalue']==True:
        success, pval, _ = runs_test(bits)
        if dic_checklist['5_1_runs_test_success']==True: all_features.append(1 if success==True else 0)
        if dic_checklist['5_2_runs_test_pvalue']==True: all_features.append(pval)
    if dic_checklist['6_1_random_excursion_variant_test_success']==True or dic_checklist['6_2_random_excursion_variant_test_pvalue']==True:
        success, _, *pval = random_excursion_variant_test(bits)
        if dic_checklist['6_1_random_excursion_variant_test_success']==True: all_features.append(1 if success==True else 0)
        if dic_checklist['6_2_random_excursion_variant_test_pvalue']==True:
            for pv in pval[0]: all_features.append(pv) #18 values
    if dic_checklist['8_1_cumulative_sums_test_success']==True or dic_checklist['8_2_cumulative_sums_test_pvalue']==True:
        success, _, *pval = cumulative_sums_test(bits)
        if dic_checklist['8_1_cumulative_sums_test_success']==True: all_features.append(1 if success==True else 0)
        if dic_checklist['8_2_cumulative_sums_test_pvalue']==True:
            for pv in pval[0]: all_features.append(pv) #2 values
    if dic_checklist['9_1_non_overlapping_template_matching_test_success']==True or dic_checklist['9_2_non_overlapping_template_matching_test_pvalue']==True:
        success, pval, _ = non_overlapping_template_matching_test(bits)
        if dic_checklist['9_1_non_overlapping_template_matching_test_success']==True: all_features.append(1 if success==True else 0)
        if dic_checklist['9_2_non_overlapping_template_matching_test_pvalue']==True: all_features.append(pval)
    if dic_checklist['10_1_monobit_test_success']==True or dic_checklist['10_2_monobit_test_pvalue']==True:
        success, pval, _ = monobit_test(bits)
        if dic_checklist['10_1_monobit_test_success']==True: all_features.append(1 if success==True else 0)
        if dic_checklist['10_2_monobit_test_pvalue']==True: all_features.append(pval)
    if dic_checklist['11_1_random_excursion_test_success']==True or dic_checklist['11_2_random_excursion_test_pvalue']==True:
        success, _, *pval = random_excursion_test(bits)
        if dic_checklist['11_1_random_excursion_test_success']==True: all_features.append(1 if success==True else 0)
        if dic_checklist['11_2_random_excursion_test_pvalue']==True:
            for pv in pval[0]: all_features.append(pv) #8 values
    if dic_checklist['12_1_dft_test_success']==True or dic_checklist['12_2_dft_test_pvalue']==True:
        success, pval, _ = dft_test(bits)
        if dic_checklist['12_1_dft_test_success']==True: all_features.append(1 if success==True else 0)
        if dic_checklist['12_2_dft_test_pvalue']==True: all_features.append(pval)
    if dic_checklist['13_1_serial_test_success']==True or dic_checklist['13_2_serial_test_pvalue']==True:
        success, _, *pval  = serial_test(bits)
        if dic_checklist['13_1_serial_test_success']==True: all_features.append(1 if success==True else 0)
        if dic_checklist['13_2_serial_test_pvalue']==True:
            for pv in pval[0]: all_features.append(pv) #2 values
    if dic_checklist['14_1_binary_matrix_rank_test_success']==True or dic_checklist['14_2_binary_matrix_rank_test_pvalue']==True:
        success, pval, _ = binary_matrix_rank_test(bits, M=28, Q=28)
        if dic_checklist['14_1_binary_matrix_rank_test_success']==True: all_features.append(1 if success==True else 0)
        if dic_checklist['14_2_binary_matrix_rank_test_pvalue']==True: all_features.append(pval)       
    if dic_checklist["16_chi_test_absolute_success"] or dic_checklist["17_chi_test_confidence_success"] or dic_checklist["18_1_chi2_uniform_stat"] or dic_checklist["18_2_chi2_uniform_pvalue"]:
        chi2_stat, p_value = chi2_uniform(row)
        if dic_checklist["16_chi_test_absolute_success"]==True: all_features.append(chi_test_absolute(chi2_stat))
        if dic_checklist["17_chi_test_confidence_success"]==True: all_features.append(chi_test_confidence(p_value))
        if dic_checklist["18_1_chi2_uniform_stat"]==True: all_features.append(chi2_stat)
        if dic_checklist["18_2_chi2_uniform_pvalue"]==True: all_features.append(p_value)     
    if dic_checklist["19_byte_frequency_distribution"]==True: all_features.extend(byte_frequency_distribution(row))  #256 features
    if dic_checklist["20_discrete_fourier_transform"]==True: all_features.extend(discrete_fourier_transform(row)) #10 features
    if dic_checklist["21_lempel_ziv_complexity"]==True: all_features.append(lempel_ziv_complexity(row))
    if dic_checklist["22_monte_carlo_pi_approximation"]==True: all_features.append(monte_carlo_pi_approximation(row))
    
    return all_features

def generate_feature_headers(dic_checklist):
    headers = []

    # NIST features
    if dic_checklist.get('1_1_longest_run_ones_in_a_block_test_success'):
        headers.append('1_1_longest_run_ones_in_a_block_test_success')
    if dic_checklist.get('1_2_longest_run_ones_in_a_block_test_pvalue'):
        headers.append('1_2_longest_run_ones_in_a_block_test_pvalue')

    if dic_checklist.get('2_1_overlapping_template_matching_test_success'):
        headers.append('2_1_overlapping_template_matching_test_success')
    if dic_checklist.get('2_2_overlapping_template_matching_test_pvalue'):
        headers.append('2_2_overlapping_template_matching_test_pvalue')

    if dic_checklist.get('3_1_approximate_entropy_test_success'):
        headers.append('3_1_approximate_entropy_test_success')
    if dic_checklist.get('3_2_approximate_entropy_test_pvalue'):
        headers.append('3_2_approximate_entropy_test_pvalue')

    if dic_checklist.get('4_1_frequency_within_block_test_success'):
        headers.append('4_1_frequency_within_block_test_success')
    if dic_checklist.get('4_2_frequency_within_block_test_pvalue'):
        headers.append('4_2_frequency_within_block_test_pvalue')

    if dic_checklist.get('5_1_runs_test_success'):
        headers.append('5_1_runs_test_success')
    if dic_checklist.get('5_2_runs_test_pvalue'):
        headers.append('5_2_runs_test_pvalue')

    if dic_checklist.get('6_1_random_excursion_variant_test_success'):
        headers.append('6_1_random_excursion_variant_test_success')
    if dic_checklist.get('6_2_random_excursion_variant_test_pvalue'):
        for i in range(18):
            headers.append(f'6_2_random_excursion_variant_test_pvalue_{i}')

    if dic_checklist.get('8_1_cumulative_sums_test_success'):
        headers.append('8_1_cumulative_sums_test_success')
    if dic_checklist.get('8_2_cumulative_sums_test_pvalue'):
        for i in range(2):
            headers.append(f'8_2_cumulative_sums_test_pvalue_{i}')

    if dic_checklist.get('9_1_non_overlapping_template_matching_test_success'):
        headers.append('9_1_non_overlapping_template_matching_test_success')
    if dic_checklist.get('9_2_non_overlapping_template_matching_test_pvalue'):
        headers.append('9_2_non_overlapping_template_matching_test_pvalue')

    if dic_checklist.get('10_1_monobit_test_success'):
        headers.append('10_1_monobit_test_success')
    if dic_checklist.get('10_2_monobit_test_pvalue'):
        headers.append('10_2_monobit_test_pvalue')

    if dic_checklist.get('11_1_random_excursion_test_success'):
        headers.append('11_1_random_excursion_test_success')
    if dic_checklist.get('11_2_random_excursion_test_pvalue'):
        for i in range(8):
            headers.append(f'11_2_random_excursion_test_pvalue_{i}')

    if dic_checklist.get('12_1_dft_test_success'):
        headers.append('12_1_dft_test_success')
    if dic_checklist.get('12_2_dft_test_pvalue'):
        headers.append('12_2_dft_test_pvalue')

    if dic_checklist.get('13_1_serial_test_success'):
        headers.append('13_1_serial_test_success')
    if dic_checklist.get('13_2_serial_test_pvalue'):
        for i in range(2):
            headers.append(f'13_2_serial_test_pvalue_{i}')

    if dic_checklist.get('14_1_binary_matrix_rank_test_success'):
        headers.append('14_1_binary_matrix_rank_test_success')
    if dic_checklist.get('14_2_binary_matrix_rank_test_pvalue'):
        headers.append('14_2_binary_matrix_rank_test_pvalue')

    if dic_checklist.get('15_1_linear_complexity_test_success'):
        headers.append('15_1_linear_complexity_test_success')
    if dic_checklist.get('15_2_linear_complexity_test_pvalue'):
        headers.append('15_2_linear_complexity_test_pvalue')

    # HEDGE specials
    if dic_checklist.get("16_chi_test_absolute_success"):
        headers.append('16_chi_test_absolute_success')
    if dic_checklist.get("17_chi_test_confidence_success"):
        headers.append('17_chi_test_confidence_success')
    if dic_checklist.get("18_1_chi2_uniform_stat"):
        headers.append('18_1_chi2_uniform_stat')
    if dic_checklist.get("18_2_chi2_uniform_pvalue"):
        headers.append('18_2_chi2_uniform_pvalue')
    # BLFCP specials
    if dic_checklist.get("19_byte_frequency_distribution"):
        for i in range(256):
            headers.append(f'19_byte_frequency_distribution_{i}')
    if dic_checklist.get("20_discrete_fourier_transform"):
        for i in range(10):
            headers.append(f'20_discrete_fourier_transform_{i}')
    if dic_checklist.get("21_lempel_ziv_complexity"):
        headers.append('21_lempel_ziv_complexity')
    if dic_checklist.get("22_monte_carlo_pi_approximation"):
        headers.append('22_monte_carlo_pi_approximation')

    return headers

def parallel_feature_extraction_large_file(window, input_file_path, output_file_path, dic_checklist, cancel_event, chunk_size=100):
    try:
        total_cores = mp.cpu_count()
        num_cores = max(1, int(total_cores * dic_checklist["-cpu-"]/100))
        
        window.write_event_value('-THREAD-UPDATE-', (0, 'Stage 1/2: Counting rows...', 'Calculating...'))
        with open(input_file_path, 'r', encoding='utf-8') as f:
            row_count = sum(1 for line in f)
        total_chunks = ceil(row_count / chunk_size)
        if cancel_event.is_set(): window.write_event_value('-THREAD-DONE-', False); return

        window.write_event_value('-THREAD-UPDATE-', (0, 'Stage 2/2: Processing...', ''))
        window['-PROGRESS-'].update(0, max=total_chunks) # Set max value of progress bar
        
        start_time = time.time()
        is_first_chunk = True
        header_list = generate_feature_headers(dic_checklist)
        process_row_fixed = partial(process_row, dic_checklist=dic_checklist)

        with mp.Pool(processes=num_cores) as pool:
            chunk_iterator = pd.read_csv(input_file_path, header=None, dtype=np.uint8, chunksize=chunk_size)
            for i, chunk_df in enumerate(chunk_iterator):
                if cancel_event.is_set():
                    window.write_event_value('-THREAD-DONE-', False)
                    return
                
                data_rows = [row for _, row in chunk_df.iterrows()]
                results = pool.map(process_row_fixed, data_rows)
                
                features_df = pd.DataFrame(results)
                features_df.to_csv(output_file_path, index=False,
                    mode='w' if is_first_chunk else 'a',
                    header=header_list if is_first_chunk else False)
                is_first_chunk = False
                
                chunk_written = i + 1
                elapsed_time = time.time() - start_time
                progress_percent = chunk_written / total_chunks if total_chunks > 0 else 0
                etr = (elapsed_time / progress_percent) - elapsed_time if progress_percent > 0 else 0
                
                status = f'Writing chunk {chunk_written} of {total_chunks}'
                time_str = f"Elapsed: {format_time(elapsed_time)} | ETR: {format_time(etr)}"
                window.write_event_value('-THREAD-UPDATE-', (chunk_written, status, time_str))

        window.write_event_value('-THREAD-DONE-', True)
    except Exception as e:
        # Send any error back to the main thread to be displayed in a popup
        psg.popup_error(f"An error occurred in the worker thread:\n\n{e}", title="Error")
        window.write_event_value('-THREAD-DONE-', False)

def button_5():
    layout = [
        [psg.VPush()],
        [psg.Text("first csv"), psg.Input(key="-INP1B1-", readonly=True, disabled_readonly_background_color=readonly_bgcolor, disabled_readonly_text_color=readonly_textcolor, expand_x=True),psg.FileBrowse(file_types=((('CSV Files', '*.csv'),)))],
        [psg.Text("second csv"), psg.Input(key="-INP2B1-", readonly=True, disabled_readonly_background_color=readonly_bgcolor, disabled_readonly_text_color=readonly_textcolor, expand_x=True),psg.FileBrowse(file_types=((('CSV Files', '*.csv'),)))],
        [psg.Column([[psg.Checkbox("header", default=True, key="-INP3B1-",  )]], expand_x=True, element_justification='center' )],
        [psg.Text("destination file folder", tooltip="the file name and path that you want to save combined csv files to"), psg.Input(key="-INP4B1-", readonly=True, disabled_readonly_background_color=readonly_bgcolor, disabled_readonly_text_color=readonly_textcolor, expand_x=True), psg.FileSaveAs(file_types=((('CSV Files', '*.csv'),)))],
        [psg.Column([[psg.Button("combine", key="-ETC-")]], expand_x=True, element_justification='center' )],
        [psg.VPush()],
    ]
    window = psg.Window("combine csv files along their rows", layout, resizable=True)
    while True:
        event, values = window.read()
        if event in ["Exit", psg.WIN_CLOSED]: break
        if event=="-ETC-":
            layout_2 = [
                [psg.Text("combine csv files along their rows.")],
                [psg.Text("Status:"), psg.Text("Idle", key='-STATUS-')],
                [psg.ProgressBar(1, orientation='h', size=(50, 20), key='-PROGRESS-')],
                [psg.Text("", key='-TIME-')],
                [psg.Button("Start"), psg.Button("Cancel", key="-CNL-", disabled=True)]
            ]
            window_2 = psg.Window("Fragment Extractor", layout_2)
            while True:
                event_2, values_2 = window_2.read()
                if event_2 in ["Exit", psg.WIN_CLOSED]: break
                if event_2 == 'Start':
                    window_2['-CNL-'].update(disabled=False)
                    window_2['Start'].update(disabled=True)
                    success = concatenate_rows_gui(window_2, [values["-INP1B1-"], values["-INP2B1-"]], values["-INP4B1-"], header=values["-INP3B1-"] )
                    if success: psg.popup("Done!", "The process completed successfully."); break
                    else: psg.popup("Cancelled", "The process was cancelled by the user."); break
                    window_2['-CNL-'].update(disabled=True)
                    window_2['Start'].update(disabled=False)
            window_2.close()
            window.close()
    window.close()

def button_6():
    layout = [
        [psg.VPush()],
        [psg.Text("first csv"), psg.Input(key="-INP1B1-", readonly=True, disabled_readonly_background_color=readonly_bgcolor, disabled_readonly_text_color=readonly_textcolor, expand_x=True),psg.FileBrowse(file_types=((('CSV Files', '*.csv'),)))],
        [psg.Text("second csv"), psg.Input(key="-INP2B1-", readonly=True, disabled_readonly_background_color=readonly_bgcolor, disabled_readonly_text_color=readonly_textcolor, expand_x=True),psg.FileBrowse(file_types=((('CSV Files', '*.csv'),)))],
        [psg.Column([[psg.Checkbox("header", default=True, key="-INP3B1-",  )]], expand_x=True, element_justification='center' )],
        [psg.Text("destination file folder", tooltip="the file name and path that you want to save combined csv files to"), psg.Input(key="-INP4B1-", readonly=True, disabled_readonly_background_color=readonly_bgcolor, disabled_readonly_text_color=readonly_textcolor, expand_x=True), psg.FileSaveAs(file_types=((('CSV Files', '*.csv'),)))],
        [psg.Column([[psg.Button("combine", key="-ETC-")]], expand_x=True, element_justification='center' )],
        [psg.VPush()],
    ]
    window = psg.Window("combine csv files along their columns", layout, resizable=True)
    while True:
        event, values = window.read()
        if event in ["Exit", psg.WIN_CLOSED]: break
        if event=="-ETC-":
            layout_2 = [
                [psg.Text("combine csv files along their columns.")],
                [psg.Text("Status:"), psg.Text("Idle", key='-STATUS-')],
                [psg.ProgressBar(1, orientation='h', size=(50, 20), key='-PROGRESS-')],
                [psg.Text("", key='-TIME-')],
                [psg.Button("Start"), psg.Button("Cancel", key="-CNL-", disabled=True)]
            ]
            window_2 = psg.Window("Fragment Extractor", layout_2)
            while True:
                event_2, values_2 = window_2.read()
                if event_2 in ["Exit", psg.WIN_CLOSED]: break
                if event_2 == 'Start':
                    window_2['-CNL-'].update(disabled=False)
                    window_2['Start'].update(disabled=True)
                    success = concatenate_columns_gui(window_2, [values["-INP1B1-"], values["-INP2B1-"]], values["-INP4B1-"], header=values["-INP3B1-"] )
                    if success: psg.popup("Done!", "The process completed successfully."); break
                    else: psg.popup("Cancelled", "The process was cancelled by the user."); break
                    window_2['-CNL-'].update(disabled=True)
                    window_2['Start'].update(disabled=False)
            window_2.close()
            window.close()
    window.close()
# Your original main execution block, untouched (with mp.freeze_support() added for safety)
if __name__ == '__main__':
    mp.freeze_support() # Recommended for multiprocessing, especially for executables
    menu_def = [
    ['&options', ['&guidance', '&about']],
    ]
    ltab_extract_file = [
        [psg.VPush()],
        [psg.Push(), psg.Column([[psg.Button("extract N fragments from whole folder randomly", key='-BTN1-', button_color=NORMAL_COLOR)], [psg.Button("extract M fragments from each file randomly", key='-BTN2-', button_color=NORMAL_COLOR)], [psg.Button("extract M fragments from each file randomly with padding", key='-BTN3-', button_color=NORMAL_COLOR)]], element_justification='c'), psg.Push()],
        [psg.VPush()],
    ]
    tab_extract_file = psg.Tab("fragment extractor", ltab_extract_file)
    ltab_feature_extractor= [
        [psg.Text("⚠️ WARNING: for now this version of  app's features extraction", text_color='yellow', font=('Helvetica', 10, 'bold'))],
        [psg.Text('\t          is usable and optimized for 4096 bytes fragments!', text_color='yellow', font=('Helvetica', 10, 'bold'))],
        [psg.VPush()],
        [psg.Push(), psg.Column([[psg.Button("extract features", key='-BTN4-', button_color=NORMAL_COLOR)],], element_justification='c'), psg.Push()],
        [psg.VPush()],
    ]
    tab_feature_extractor = psg.Tab("feature extractor", ltab_feature_extractor)
    ltab_combine_csv = [
        [psg.VPush()],
        [psg.Push(), psg.Column([[psg.Button("combine csv files along their rows", key='-BTN5-', button_color=NORMAL_COLOR)], [psg.Button("combine csv files along their columns", key='-BTN6-', button_color=NORMAL_COLOR)],], element_justification='c'), psg.Push()],
        [psg.VPush()],
    ]
    tab_combine_csv = psg.Tab("combine csv files", ltab_combine_csv)
    
    ltab_group = [[tab_extract_file, tab_feature_extractor, tab_combine_csv]]
    main_layout = [
        [psg.Menu(menu_def, background_color='#3C3C3C', text_color='#FFFFFF', tearoff=False)],
        [psg.TabGroup(ltab_group, tab_location="center", expand_x=True, expand_y=True)]
        ]



    window = psg.Window('Enc fragment extractor', main_layout, size=(500,500), resizable=True, finalize=True,)

    # --- START of THE GARBAGE COLLECTION FIX ---
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        icon_path = os.path.join(script_dir, "icons", "main_icon.png")

        # Open the image with Pillow and convert it for Tkinter
        image = Image.open(icon_path)
        icon_photo = ImageTk.PhotoImage(image)

        # ** THE CRITICAL FIX IS HERE **
        # Anchor the image object to the window to prevent garbage collection
        window.TKroot.icon_photo = icon_photo

        # Set the icon, using False to avoid a potential Windows bug [32]
        window.TKroot.iconphoto(False, icon_photo)

    except Exception as e:
        print(f"--- ICON FAILED TO LOAD ---")
        print(f"An error occurred: {e}")
    # --- END of THE FIX ---
    
    window['-BTN1-'].bind('<Enter>', '_ENTER'); window['-BTN1-'].bind('<Leave>', '_LEAVE')
    window['-BTN2-'].bind('<Enter>', '_ENTER'); window['-BTN2-'].bind('<Leave>', '_LEAVE')
    window['-BTN3-'].bind('<Enter>', '_ENTER'); window['-BTN3-'].bind('<Leave>', '_LEAVE')
    window['-BTN4-'].bind('<Enter>', '_ENTER'); window['-BTN4-'].bind('<Leave>', '_LEAVE')
    window['-BTN5-'].bind('<Enter>', '_ENTER'); window['-BTN5-'].bind('<Leave>', '_LEAVE')
    window['-BTN6-'].bind('<Enter>', '_ENTER'); window['-BTN6-'].bind('<Leave>', '_LEAVE')
    while True:
        event, values = window.read()
        if event=="about":psg.popup('Version 0.8', 'Created by alireza aliaskari hosseinabadi', button_justification="center",  grab_anywhere=True)
        if event=="guidance":
            guidance()
        if event in ["Exit", psg.WIN_CLOSED]: break
        if event == '-BTN1-_ENTER': window['-BTN1-'].update(button_color=HOVER_COLOR)
        elif event == '-BTN1-_LEAVE': window['-BTN1-'].update(button_color=NORMAL_COLOR)
        if event == '-BTN2-_ENTER': window['-BTN2-'].update(button_color=HOVER_COLOR)
        elif event == '-BTN2-_LEAVE': window['-BTN2-'].update(button_color=NORMAL_COLOR)
        if event == '-BTN3-_ENTER': window['-BTN3-'].update(button_color=HOVER_COLOR)
        elif event == '-BTN3-_LEAVE': window['-BTN3-'].update(button_color=NORMAL_COLOR)
        if event == '-BTN4-_ENTER': window['-BTN4-'].update(button_color=HOVER_COLOR)
        elif event == '-BTN4-_LEAVE': window['-BTN4-'].update(button_color=NORMAL_COLOR)
        if event == '-BTN5-_ENTER': window['-BTN5-'].update(button_color=HOVER_COLOR)
        elif event == '-BTN5-_LEAVE': window['-BTN5-'].update(button_color=NORMAL_COLOR)
        if event == '-BTN6-_ENTER': window['-BTN6-'].update(button_color=HOVER_COLOR)
        elif event == '-BTN6-_LEAVE': window['-BTN6-'].update(button_color=NORMAL_COLOR)
        if event=='-BTN1-': button_1()
        elif event=='-BTN2-': button_2()
        elif event=='-BTN3-': button_3()
        elif event=='-BTN4-': button_4()
        elif event=='-BTN5-': button_5()
        elif event=='-BTN6-': button_6()
    window.close()