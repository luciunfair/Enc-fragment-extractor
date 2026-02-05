import os
import time
import pandas as pd
import dask.dataframe as dd
from typing import List, Optional
import FreeSimpleGUI as psg

# --- Helper Function (from your example) ---
def format_time(seconds: float) -> str:
    """Formats seconds into an HH:MM:SS string."""
    if seconds is None or seconds < 0:
        return "Calculating..."
    
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

# --- GUI-Integrated Functions ---

def concatenate_columns_gui(window: psg.Window, file_list: List[str], output_file: str, sep: str = ',', header: bool = True) -> bool:
    """
    Concatenates CSV files along columns using Dask, with GUI feedback.
    Note: Progress is not granular as Dask's `to_csv` is a single blocking operation.
    """
    try:
        if not file_list:
            psg.popup_error("Error: The list of files is empty.")
            return False

        total_files = len(file_list)
        window['-STATUS-'].update(f'Preparing to concatenate {total_files} files by column...')
        window['-PROGRESS-'].update(0, max=1) # Simple 0 to 1 progress
        window.refresh() # Ensure GUI updates before the blocking call
        start_time = time.time()

        # Check for cancellation event before starting the blocking operation
        event, _ = window.read(timeout=0)
        if event in (psg.WIN_CLOSED, '-CNL-'):
            psg.popup("Cancelled", "The process was cancelled before starting.")
            return False

        # --- Dask's Blocking Operation ---
        window['-STATUS-'].update('Dask is processing... This may take a while.')
        window.refresh()
        
        read_header = 0 if header else None
        dask_dfs = [dd.read_csv(file, header=read_header) for file in file_list]
        concatenated_ddf = dd.concat(dask_dfs, axis=1)
        concatenated_ddf.to_csv(output_file, sep=sep, header=header, single_file=True, index=False)
        # --- End of Blocking Operation ---

        elapsed_time = time.time() - start_time
        window['-PROGRESS-'].update(1)
        window['-STATUS-'].update('Completed successfully!')
        window['-TIME-'].update(f"Total Time: {format_time(elapsed_time)}")
        psg.popup("Success!", f"Successfully concatenated files along columns to:\n'{output_file}'.")
        return True
    
    except Exception as e:
        psg.popup_error(f"An unexpected error occurred: {e}")
        return False

def concatenate_columns_gui(window: psg.Window, file_list: List[str], output_file: str, chunk_size: int = 10000, sep: str = ',', header: bool = True) -> bool:
    """
    Concatenates files Side-by-Side (Axis=1) using the exact same inputs as the row function.
    """
    try:
        if not file_list:
            psg.popup_error("Error: The list of files is empty.")
            return False

        # 1. Determine Header Handling based on input
        # If header=True, we read the first row as header. If False, we assume no header exists.
        read_header_arg = 0 if header else None
        
        window['-STATUS-'].update('Preparing to merge columns...')
        window['-PROGRESS-'].update(0, max=100) # Indeterminate progress for zip operations
        
        # 2. Create Iterators for ALL files simultaneously
        # We pass 'sep' and 'header' here to respect your inputs
        file_iterators = [
            pd.read_csv(f, chunksize=chunk_size, sep=sep, header=read_header_arg, dtype=str) 
            for f in file_list
        ]
        
        first_chunk = True
        
        # 3. Open Output File
        with open(output_file, 'w', newline='', encoding='utf-8') as f_out:
            
            # 4. Iterate through all files in lock-step
            for i, chunks in enumerate(zip(*file_iterators)):
                
                # GUI Responsiveness check
                event, _ = window.read(timeout=0)
                if event in (psg.WIN_CLOSED, '-CNL-'):
                    psg.popup("Cancelled", "Process stopped by user.")
                    return False

                # Align indices to ensure safe concatenation (ignores row index numbers)
                for c in chunks:
                    c.reset_index(drop=True, inplace=True)
                
                # Concatenate the chunks side-by-side
                merged_chunk = pd.concat(chunks, axis=1)
                
                # Write to CSV
                # We only write the header if it's the very first chunk AND header=True was requested
                write_header = header if first_chunk else False
                
                merged_chunk.to_csv(f_out, sep=sep, index=False, header=write_header, encoding='utf-8')
                
                first_chunk = False
                window['-STATUS-'].update(f'Merged chunk batch {i+1}...')
                
        window['-STATUS-'].update('Completed successfully!')
        window['-PROGRESS-'].update(100)
        psg.popup("Success!", f"Files merged side-by-side to:\n'{output_file}'")
        return True

    except ValueError as e:
        # This catches the specific error if files have different row counts
        psg.popup_error(f"Merge Error: Files likely have different row counts.\nDetails: {e}")
        return False
    except Exception as e:
        psg.popup_error(f"An unexpected error occurred: {e}")
        return False