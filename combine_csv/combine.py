import os
import time
import pandas as pd
from typing import List
import FreeSimpleGUI as psg


def format_time(seconds: float) -> str:
    """Formats seconds into an HH:MM:SS string."""
    if seconds is None or seconds < 0:
        return "Calculating..."
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def concatenate_rows_gui(window: psg.Window, file_list: List[str], output_file: str, chunk_size: int = 10000, sep: str = ',', header: bool = True) -> bool:
    """
    Concatenates CSV files vertically along rows (Axis = 0) in memory-safe chunks
    with live GUI progress bar and cancellation support.
    """
    try:
        if not file_list or len(file_list) < 2:
            psg.popup_error("Error: Please select at least two CSV files to combine.")
            return False

        read_header_arg = 0 if header else None
        window['-STATUS-'].update('Counting rows across input CSV files...')
        window['-PROGRESS-'].update(0, max=100)
        window.refresh()

        total_rows = 0
        for fpath in file_list:
            with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
                cnt = sum(1 for _ in f)
                if header and cnt > 0:
                    cnt -= 1
                total_rows += max(0, cnt)

        total_batches = max(1, (total_rows + chunk_size - 1) // chunk_size)
        window['-PROGRESS-'].update(0, max=total_batches)

        start_time = time.time()
        first_write = True
        expected_cols = None
        batch_idx = 0

        with open(output_file, 'w', newline='', encoding='utf-8') as f_out:
            for file_idx, fpath in enumerate(file_list):
                reader = pd.read_csv(fpath, chunksize=chunk_size, sep=sep, header=read_header_arg, dtype=str)
                for chunk in reader:
                    event, _ = window.read(timeout=0)
                    if event in (psg.WIN_CLOSED, '-CNL-'):
                        psg.popup("Cancelled", "Row merge stopped by user.")
                        return False

                    if expected_cols is None:
                        expected_cols = chunk.shape[1]
                    elif chunk.shape[1] != expected_cols:
                        psg.popup_error(
                            f"Column Count Mismatch!\nFile '{os.path.basename(fpath)}' has {chunk.shape[1]} columns, "
                            f"expected {expected_cols} columns.",
                            title="Merge Error"
                        )
                        return False

                    write_hdr = header if first_write else False
                    chunk.to_csv(f_out, sep=sep, index=False, header=write_hdr, encoding='utf-8')
                    first_write = False
                    batch_idx += 1

                    elapsed = time.time() - start_time
                    pct = min(1.0, batch_idx / float(total_batches))
                    etr = (elapsed / pct) - elapsed if pct > 0 else 0.0
                    window['-PROGRESS-'].update(min(batch_idx, total_batches))
                    window['-STATUS-'].update(f"Appending file {file_idx + 1}/{len(file_list)} (batch {batch_idx})...")
                    window['-TIME-'].update(f"Elapsed: {format_time(elapsed)} | ETR: {format_time(etr)}")

        elapsed = time.time() - start_time
        window['-PROGRESS-'].update(total_batches)
        window['-STATUS-'].update('Completed successfully!')
        window['-TIME-'].update(f"Total Time: {format_time(elapsed)}")
        psg.popup("Success!", f"Files stacked along rows to:\n'{output_file}'")
        return True

    except Exception as e:
        psg.popup_error(f"An unexpected error occurred during row merge:\n{e}")
        return False


def concatenate_columns_gui(window: psg.Window, file_list: List[str], output_file: str, chunk_size: int = 10000, sep: str = ',', header: bool = True) -> bool:
    """
    Concatenates CSV files side-by-side along columns (Axis = 1) in memory-safe chunks
    with strict row-count validation and live GUI progress feedback.
    """
    try:
        if not file_list or len(file_list) < 2:
            psg.popup_error("Error: Please select at least two CSV files to combine.")
            return False

        read_header_arg = 0 if header else None
        window['-STATUS-'].update('Validating row counts across files...')
        window['-PROGRESS-'].update(0, max=100)
        window.refresh()

        row_counts = []
        for fpath in file_list:
            with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
                cnt = sum(1 for _ in f)
                if header and cnt > 0:
                    cnt -= 1
                row_counts.append(cnt)

        if len(set(row_counts)) > 1:
            psg.popup_error(
                f"Merge Error: Input files have different row counts ({row_counts}).\n"
                "Side-by-side column merge requires identical row counts.",
                title="Row Count Mismatch"
            )
            return False

        total_batches = max(1, (row_counts[0] + chunk_size - 1) // chunk_size)
        window['-PROGRESS-'].update(0, max=total_batches)

        file_iterators = [
            pd.read_csv(f, chunksize=chunk_size, sep=sep, header=read_header_arg, dtype=str)
            for f in file_list
        ]

        start_time = time.time()
        first_chunk = True

        with open(output_file, 'w', newline='', encoding='utf-8') as f_out:
            for i, chunks in enumerate(zip(*file_iterators)):
                event, _ = window.read(timeout=0)
                if event in (psg.WIN_CLOSED, '-CNL-'):
                    psg.popup("Cancelled", "Column merge stopped by user.")
                    return False

                for c in chunks:
                    c.reset_index(drop=True, inplace=True)

                merged_chunk = pd.concat(chunks, axis=1)
                write_header = header if first_chunk else False
                merged_chunk.to_csv(f_out, sep=sep, index=False, header=write_header, encoding='utf-8')
                first_chunk = False

                batch_num = i + 1
                elapsed = time.time() - start_time
                pct = min(1.0, batch_num / float(total_batches))
                etr = (elapsed / pct) - elapsed if pct > 0 else 0.0
                window['-PROGRESS-'].update(min(batch_num, total_batches))
                window['-STATUS-'].update(f'Merged column batch {batch_num} of {total_batches}...')
                window['-TIME-'].update(f"Elapsed: {format_time(elapsed)} | ETR: {format_time(etr)}")

        elapsed = time.time() - start_time
        window['-STATUS-'].update('Completed successfully!')
        window['-PROGRESS-'].update(total_batches)
        window['-TIME-'].update(f"Total Time: {format_time(elapsed)}")
        psg.popup("Success!", f"Files merged side-by-side to:\n'{output_file}'")
        return True

    except Exception as e:
        psg.popup_error(f"An unexpected error occurred during column merge:\n{e}")
        return False