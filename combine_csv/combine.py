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

def concatenate_rows_gui(window: psg.Window, file_list: List[str], output_file: str, chunk_size: int = 10000, sep: str = ',', header: bool = True) -> bool:
    """
    Concatenates CSV files along rows using a chunking approach with detailed GUI feedback.
    """
    try:
        if not file_list:
            psg.popup_error("Error: The list of files is empty.")
            return False

        # Calculate total size for accurate progress bar
        total_size = sum(os.path.getsize(f) for f in file_list)
        processed_size = 0
        
        window['-STATUS-'].update(f'Preparing to concatenate {len(file_list)} files by row...')
        window['-PROGRESS-'].update(0, max=total_size)
        start_time = time.time()
        
        read_header = 0 if header else None
        
        # Open the output file once
        with open(output_file, 'w', newline='') as f_out:
            # Process the first file to handle the header correctly
            first_file = file_list[0]
            
            # Use a for loop for chunk processing to handle cancellation
            for i, chunk in enumerate(pd.read_csv(first_file, chunksize=chunk_size, header=read_header)):
                event, _ = window.read(timeout=0)
                if event in (psg.WIN_CLOSED, '-CNL-'):
                    psg.popup("Cancelled", "The process was cancelled by the user.")
                    return False
                
                # Write the first chunk with or without header, subsequent chunks without
                write_header = header if i == 0 else False
                chunk.to_csv(f_out, sep=sep, index=False, header=write_header)

            processed_size += os.path.getsize(first_file)

            # Process the rest of the files
            for file_idx, file in enumerate(file_list[1:], start=1):
                window['-STATUS-'].update(f'Processing file {file_idx+1}/{len(file_list)}: {os.path.basename(file)}')
                
                for chunk in pd.read_csv(file, chunksize=chunk_size, header=read_header):
                    event, _ = window.read(timeout=0)
                    if event in (psg.WIN_CLOSED, '-CNL-'):
                        psg.popup("Cancelled", "The process was cancelled by the user.")
                        return False
                    chunk.to_csv(f_out, sep=sep, index=False, header=False)

                # Update progress after each file is completed
                processed_size += os.path.getsize(file)
                elapsed_time = time.time() - start_time
                progress_percent = processed_size / total_size if total_size > 0 else 0
                etr = (elapsed_time / progress_percent) - elapsed_time if progress_percent > 0.01 else None
                
                window['-PROGRESS-'].update(processed_size)
                window['-TIME-'].update(f"Elapsed: {format_time(elapsed_time)} | ETR: {format_time(etr)}")
        
        window['-STATUS-'].update('Completed successfully!')
        window['-PROGRESS-'].update(total_size)
        window['-TIME-'].update(f"Total Time: {format_time(time.time() - start_time)}")
        psg.popup("Success!", f"Successfully concatenated files along rows to:\n'{output_file}'.")
        return True

    except Exception as e:
        psg.popup_error(f"An unexpected error occurred: {e}")
        return False

