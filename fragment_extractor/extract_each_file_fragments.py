import os
import random
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

def extract_random_fragments_file(window, folder_path, fragment_size, num_fragments, dest_csv):
    total_small_frag = 0

    # Open the destination CSV file in write mode
    with open(dest_csv, 'w', newline='') as csvfile:
        
        # Create a csv writer object
        csv_writer = csv.writer(csvfile)
        
        # Iterate through all files in the specified folder
        folder_path_adrs = os.listdir(folder_path)
        
        total_fragments = num_fragments * len(folder_path_adrs)
        window['-STATUS-'].update(f'Writing {total_fragments} fragments...')
        window['-PROGRESS-'].update(0, max=total_fragments) # Reset progress bar for the second stage
        
        fragments_written = 0
        start_time = time.time()
        for i, filename in enumerate(folder_path_adrs):
            file_path = os.path.join(folder_path, filename)
            event, _ = window.read(timeout=0)
            if event in (psg.WIN_CLOSED, '-CNL-'):
                psg.popup("Cancelled", "The process was cancelled by the user during Stage .")
                return False
            # Check if it's a file and not a directory
            if os.path.isfile(file_path):
                try:
                    with open(file_path, 'rb') as file:
                        # Read the entire file content
                        file_content = file.read()
                        
                        # Check if the file is large enough for at least one full fragment
                        if len(file_content) < fragment_size:
                            print(f"Skipping small file (less than {fragment_size} bytes): {file_path}")
                            total_small_frag += 1
                            continue # Move to the next file

                        # Split the content into chunks of fragment_size
                        # Note: This list comprehension can still be memory intensive for very large files.
                        # For extremely large files, a generator approach might be better.
                        chunks = [file_content[i:i + fragment_size] for i in range(0, len(file_content) - fragment_size, fragment_size)]
                        
                        # Ensure there are enough chunks to select from
                        if len(chunks) >= num_fragments:
                            # Select random chunks
                            selected_chunks = random.sample(chunks, num_fragments)
                            
                            # Process each chunk and write it to the CSV immediately
                            for chunk in selected_chunks:
                                decimal_values = list(chunk)  # Convert bytes to a list of integers
                                csv_writer.writerow(decimal_values) # Write the row directly to the CSV
                        else:
                            #print(f"File too small to extract {num_fragments} fragments: {file_path}")
                            total_small_frag += 1
                            
                        fragments_written += num_fragments
                        # Update GUI periodically to avoid slowdown

                        elapsed_time = time.time() - start_time
                        progress_percent = fragments_written / total_fragments
                        etr = (elapsed_time / progress_percent) - elapsed_time if progress_percent > 0 else None
                        
                        window['-PROGRESS-'].update(fragments_written)
                        window['-STATUS-'].update(f'Writing fragment {fragments_written} of {total_fragments}')
                        window['-TIME-'].update(f"Elapsed: {format_time(elapsed_time)} | ETR: {format_time(etr)}")
                        
                except Exception as e:
                    psg.popup_error(f"Error processing file {file_path}: {e}")
                    return False

    window['-STATUS-'].update('Completed successfully!')
    window['-PROGRESS-'].update(total_fragments) # Fill the bar at the end
    window['-TIME-'].update(f"Total Time: {format_time(time.time() - start_time)}")
    psg.popup(f"Total files too small to process: {total_small_frag} out of {total_fragments}")
    return True

def extract_random_fragments_file_random_pad(window, folder_path, fragment_size, num_fragments, dest_csv):
    """
    Extracts random fragments from files, pads the last fragment if necessary,
    and writes each fragment's decimal values directly to a CSV file.
    """
    files_too_small = 0

    # Open the destination CSV file once in write mode
    with open(dest_csv, 'w', newline='') as csvfile:
        # Create a writer object to handle CSV formatting
        csv_writer = csv.writer(csvfile)
        
        # Iterate through all files in the specified folder
        folder_path_adrs = os.listdir(folder_path)
        
        total_fragments = num_fragments * len(folder_path_adrs)
        window['-STATUS-'].update(f'Writing {total_fragments} fragments...')
        window['-PROGRESS-'].update(0, max=total_fragments) # Reset progress bar for the second stage
        
        fragments_written = 0
        start_time = time.time()
        # Iterate through all files in the specified folder
        for filename in folder_path_adrs:
            file_path = os.path.join(folder_path, filename)
            event, _ = window.read(timeout=0)
            if event in (psg.WIN_CLOSED, '-CNL-'):
                psg.popup("Cancelled", "The process was cancelled by the user during Stage .")
                return False
            # Ensure it's a file, not a directory
            if os.path.isfile(file_path):
                try:
                    with open(file_path, 'rb') as file:
                        file_content = file.read()
                        
                        # Skip empty files
                        if not file_content:
                            print(f"Skipping empty file: {filename}")
                            continue

                        # --- Process file into chunks with padding ---
                        chunks = []
                        for i in range(0, len(file_content), fragment_size):
                            chunk = file_content[i:i + fragment_size]
                            # Pad the chunk if it's smaller than the desired size
                            if len(chunk) < fragment_size:
                                padding_needed = fragment_size - len(chunk)
                                # Generate random bytes for padding
                                padding = bytes(random.randint(0, 255) for _ in range(padding_needed))
                                chunk += padding
                            chunks.append(chunk)
                        # ---------------------------------------------
                        
                        # Ensure there are enough chunks to select from
                        if len(chunks) >= num_fragments:
                            # Select random chunks from the list
                            selected_chunks = random.sample(chunks, num_fragments)
                            
                            # Convert each selected chunk and write it to the CSV immediately
                            for chunk in selected_chunks:
                                decimal_values = list(chunk)  # Convert bytes to a list of integers
                                csv_writer.writerow(decimal_values) # ✨ Write the row directly to the file
                        else:
                            # This case is less likely with padding, but possible if num_fragments > 1 for a small file
                            #print(f"Not enough chunks ({len(chunks)}) in {filename} to select {num_fragments}.")
                            files_too_small += 1
                        
                        fragments_written += num_fragments
                        # Update GUI periodically to avoid slowdown

                        elapsed_time = time.time() - start_time
                        progress_percent = fragments_written / total_fragments
                        etr = (elapsed_time / progress_percent) - elapsed_time if progress_percent > 0 else None
                        
                        window['-PROGRESS-'].update(fragments_written)
                        window['-STATUS-'].update(f'Writing fragment {fragments_written} of {total_fragments}')
                        window['-TIME-'].update(f"Elapsed: {format_time(elapsed_time)} | ETR: {format_time(etr)}")
                except Exception as e:
                    psg.popup_error(f"Error processing file {file_path}: {e}")
                    return False
    
    window['-STATUS-'].update('Completed successfully!')
    window['-PROGRESS-'].update(total_fragments) # Fill the bar at the end
    window['-TIME-'].update(f"Total Time: {format_time(time.time() - start_time)}")
    psg.popup(f"Total files too small to process: {files_too_small} out of {total_fragments}")
    return True

# --- Example Usage ---
# Create a dummy folder and files for testing
# if not os.path.exists('test_folder_padded'):
#     os.makedirs('test_folder_padded')
# with open('test_folder_padded/file1.bin', 'wb') as f:
#     f.write(os.urandom(20000)) # Will create 2 full chunks and 1 padded chunk
# with open('test_folder_padded/file2.bin', 'wb') as f:
#     f.write(os.urandom(5000))  # Will create 1 padded chunk
#
# extract_random_fragments_random_pad('test_folder_padded', num_fragments=1)