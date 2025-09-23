import random
import os
import csv
import time
import FreeSimpleGUI as psg


def format_time(seconds):
    """Formats seconds into an HH:MM:SS string."""
    if seconds is None or seconds < 0:
        return "Calculating..."
    
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def extract_whole_folder_fragments(window, folder_path, fragment_size, total_fragments, dest_csv):
    """
    Extracts random fragments from files in a folder and updates a PySimpleGUI window with progress.
    Returns True on success, False on cancellation.
    """
    # --- Stage 1: Collect file metadata ---
    window['-STATUS-'].update('Stage 1 of 2: Scanning files...')
    chunk_positions = []
    try:
        addresses = os.listdir(folder_path)
    except FileNotFoundError:
        psg.popup_error(f"Folder not found: {folder_path}")
        return False
        
    window['-PROGRESS-'].update(0, max=len(addresses)) # Set max for the first stage

    for i, filename in enumerate(addresses):
        # Check for user cancellation
        event, _ = window.read(timeout=0)
        # --- FIX: Changed '_CNL_' to '-CNL-' to match the GUI ---
        if event in (psg.WIN_CLOSED, '-CNL-'):
            psg.popup("Cancelled", "The process was cancelled by the user during Stage 1.")
            return False
            
        full_path = os.path.join(folder_path, filename)
        try:
            file_size = os.path.getsize(full_path)
            if file_size >= fragment_size:
                num_chunks = (file_size // fragment_size)
                for j in range(num_chunks):
                    offset = j * fragment_size
                    chunk_positions.append((full_path, offset))
        except (OSError, PermissionError):
            continue # Skip unreadable files or directories
        
        window['-PROGRESS-'].update(i + 1)

    # --- Pre-computation and validation ---
    total_chunks = len(chunk_positions)
    if total_fragments > total_chunks:
        psg.popup_error(f"Only {total_chunks} chunks are available, but you requested {total_fragments}.")
        return False
    
    selected_indices = set(random.sample(range(total_chunks), total_fragments))
    
    # --- Stage 2: Write selected fragments to CSV ---
    window['-STATUS-'].update(f'Stage 2 of 2: Writing {total_fragments} fragments...')
    window['-PROGRESS-'].update(0, max=total_fragments) # Reset progress bar for the second stage
    
    fragments_written = 0
    start_time = time.time()
    
    with open(dest_csv, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        for idx in selected_indices:
            # Check for user cancellation
            event, _ = window.read(timeout=0)
            if event in (psg.WIN_CLOSED, '-CNL-'):
                psg.popup("Cancelled", "The process was cancelled by the user during Stage 2.")
                return False

            filepath, offset = chunk_positions[idx]
            try:
                with open(filepath, 'rb') as f:
                    f.seek(offset)
                    chunk = f.read(fragment_size)
                    writer.writerow(list(chunk))
                
                fragments_written += 1
                
                # Update GUI periodically to avoid slowdown
                if fragments_written % 250 == 0 or fragments_written==2:
                    elapsed_time = time.time() - start_time
                    progress_percent = fragments_written / total_fragments
                    etr = (elapsed_time / progress_percent) - elapsed_time if progress_percent > 0 else None
                    
                    window['-PROGRESS-'].update(fragments_written)
                    window['-STATUS-'].update(f'Writing fragment {fragments_written} of {total_fragments}')
                    window['-TIME-'].update(f"Elapsed: {format_time(elapsed_time)} | ETR: {format_time(etr)}")

            except (IOError, OSError, PermissionError) as e:
                print(f"Skipping fragment due to error: {e}")
                continue
                
    window['-STATUS-'].update('Completed successfully!')
    window['-PROGRESS-'].update(total_fragments) # Fill the bar at the end
    window['-TIME-'].update(f"Total Time: {format_time(time.time() - start_time)}")
    return True

