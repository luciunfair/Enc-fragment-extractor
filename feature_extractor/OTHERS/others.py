import numpy as np
from typing import Union
import pandas as pd
import sys

def shannon_entropy(data: Union[list, np.ndarray]) -> float:
    """
    Calculate Shannon entropy of a byte array.
    
    Shannon entropy measures the average information content (randomness)
    in the data. Higher entropy = more random.
    
    Args:
        data: Array of bytes (values 0-255) or any integer values
        
    Returns:
        Entropy in bits (for bytes: 0 to 8)
    """
    # Convert to numpy array if needed
    data = np.asarray(data, dtype=np.uint8)
    
    # Count frequency of each value
    value_counts = np.bincount(data, minlength=256)
    
    # Calculate probabilities
    probabilities = value_counts / len(data)
    
    # Remove zero probabilities (to avoid log(0))
    probabilities = probabilities[probabilities > 0]
    
    # Calculate entropy
    entropy = -np.sum(probabilities * np.log2(probabilities))
    
    return entropy

def kl_divergence_from_uniform(fragment: np.ndarray) -> float:
    """
    Calculate KL divergence from uniform distribution (FIXED for NaN).
    
    Args:
        fragment: Numpy array of bytes (values 0-255)
        
    Returns:
        Float: KL divergence value in bits
    """
    fragment = np.asarray(fragment, dtype=np.uint8)
    
    # Count byte frequencies
    byte_counts = np.bincount(fragment, minlength=256)
    
    # Convert to probabilities
    p = byte_counts / len(fragment)
    
    # Uniform distribution
    q = np.ones(256) / 256
    
    # IMPORTANT: Only calculate for non-zero probabilities in p
    # This avoids log(0) = -infinity = NaN
    mask = p > 0  # Only where p is non-zero
    
    # Calculate KL divergence only for non-zero p values
    kl = np.sum(p[mask] * np.log2(p[mask] / q[mask]))
    
    return float(kl)

# 1. Optimized Basic LCS (O(m*n) time, O(min(m, n)) space)
def lcs_length(X, Y):
    """
    Computes the length of the Longest Common Subsequence (LCS) 
    between two byte sequences X and Y using space-optimized Dynamic Programming.
    """
    # Standardize input to NumPy uint8 array
    X_arr = np.asarray(X, dtype=np.uint8)
    Y_arr = np.asarray(Y, dtype=np.uint8)
    m, n = len(X_arr), len(Y_arr)

    # Ensure n <= m for O(min(m, n)) space complexity (swap if necessary)
    if n > m:
        X_arr, Y_arr = Y_arr, X_arr
        m, n = n, m
    
    # dp_prev will hold dp[i-1] values, dp_curr will hold dp[i] values.
    # We only need two rows, which saves significant memory.
    dp_prev = np.zeros(n + 1, dtype=np.int32)
    
    for i in range(1, m + 1):
        # Initialize the current row (dp[i]) for the current byte X[i-1]
        dp_curr = np.zeros(n + 1, dtype=np.int32)
        x_byte = X_arr[i - 1]
        
        for j in range(1, n + 1):
            if x_byte == Y_arr[j - 1]:
                # Match: Diagonal element (i-1, j-1) + 1
                dp_curr[j] = dp_prev[j - 1] + 1
            else:
                # Mismatch: Max of element above (i-1, j) or element to the left (i, j-1)
                dp_curr[j] = max(dp_curr[j - 1], dp_prev[j])
        
        # Current row becomes the previous row for the next iteration
        dp_prev = dp_curr
        
    return dp_prev[n]

# 2. Normalized LCS (0.0-1.0)
def normalized_lcs(X, Y):
    """
    Computes the Normalized LCS (LCS length / max(len(X), len(Y))).
    """
    X_arr = np.asarray(X, dtype=np.uint8)
    Y_arr = np.asarray(Y, dtype=np.uint8)
    m, n = len(X_arr), len(Y_arr)
    
    # Handle empty sequences upfront
    max_len = max(m, n)
    if max_len == 0:
        return 0.0

    lcs_len = lcs_length(X_arr, Y_arr)
    return float(lcs_len) / max_len

# 3. Average across candidates (Vectorized for efficiency)
def average_normalized_lcs(input_seq, candidates_array):
    """
    Computes the average normalized LCS score against a set of candidates.
    """
    input_seq = np.asarray(input_seq, dtype=np.uint8)
    # Ensure candidates_array is a NumPy array, even if empty
    candidates_array = np.asarray(candidates_array, dtype=np.uint8)
    
    num_candidates = len(candidates_array)
    if num_candidates == 0:
        return 0.0
    
    input_len = len(input_seq)
    total_score = 0.0
    
    # Using a list comprehension for minor overhead reduction in the loop
    # and then summing, which can be slightly faster than repeated +=
    scores = [
        lcs_length(input_seq, candidate_seq) / max(input_len, len(candidate_seq))
        for candidate_seq in candidates_array
    ]
    
    return np.mean(scores)


def average_euclidean_distance(input_seq, candidates_array):
    """
    Calculates average Euclidean distance with safety fixes and full vectorization.
    """
    # 1. Cast to float64 to prevent uint8 overflow
    input_arr = np.asarray(input_seq, dtype=np.float64)
    candidates_arr = np.asarray(candidates_array, dtype=np.float64)
    
    # Handle empty candidates
    if len(candidates_arr) == 0:
        return 0.0

    # Handle 1D single candidate
    if candidates_arr.ndim == 1:
        candidates_arr = np.expand_dims(candidates_arr, axis=0)

    m = len(input_arr)
    n = candidates_arr.shape[1]
    
    # 2. Align dimensions if they differ (Vectorized Padding)
    if m != n:
        if m < n:
            # Pad input to match candidates' width
            # (0, n - m) adds zeros to the end
            input_arr = np.pad(input_arr, (0, n - m), 'constant')
        else:
            # Pad ALL candidates columns to match input's length
            # ((0,0), (0, m-n)) adds 0s to the right of every row, 0s to top/bottom
            candidates_arr = np.pad(candidates_arr, ((0, 0), (0, m - n)), 'constant')

    # 3. Fast vectorized calculation (now always aligned)
    # axis=1 computes norm for every row simultaneously
    distances = np.linalg.norm(candidates_arr - input_arr, axis=1)
    
    return np.mean(distances)

def hcv(data):
    """Calculate Huffman Code Length Variance"""
    byte_freqs = np.bincount(data, minlength=256)
    
    code_lengths = []
    total = len(data)
    for freq in byte_freqs:
        if freq > 0:
            prob = freq / total
            code_length = -np.log2(prob)  # Huffman tree depth
            code_lengths.append(code_length)
    
    return np.var(code_lengths)

def bpfv(data):
    # Feature 2: Byte Pair Frequency Variance
    pairs = {}
    for i in range(len(data)-1):
        pair = (data[i], data[i+1])
        pairs[pair] = pairs.get(pair, 0) + 1
    
    pair_probs = np.array(list(pairs.values())) / (len(data) - 1)
    bpfv_r = np.var(pair_probs)
    return bpfv_r

import numpy as np

def lnv(row, window_size=64):
    """
    Feature 31: Local Nibble Variance (LNV)
    
    Calculates the variance of the Chi-Square statistic for nibble (4-bit) distributions
    across sliding windows. High variance indicates structure (Compression); 
    Low variance indicates uniformity (Encryption).
    
    Parameters:
        row (numpy.ndarray or list): Input byte fragment (e.g., 4096 bytes).
        window_size (int): Size of the local analysis window (default 64).
        
    Returns:
        float: The variance of local chi-square scores.
    """
    # Ensure input is a numpy array of integers
    data = np.array(row, dtype=np.uint8)
    
    # 1. Validation: Ensure we have enough data for at least one window + variance
    # Variance requires at least 2 data points.
    # Step size is window_size // 2.
    # Minimum length needed = window_size + step_size = 1.5 * window_size
    if len(data) < (window_size * 1.5):
        return 0.0

    # 2. Create sliding windows (Vectorized for speed)
    step = window_size // 2
    # Calculate number of windows
    n_windows = (len(data) - window_size) // step + 1
    
    # Create a view of sliding windows (no data copying, very fast)
    # Shape: (n_windows, window_size)
    # This stride trick is standard numpy for sliding windows
    indexer = np.arange(window_size)[None, :] + np.arange(n_windows)[:, None] * step
    windows = data[indexer]

    # 3. The Nibble Trick (Split bytes into 4-bit chunks)
    # Shape becomes (n_windows, window_size * 2)
    high_nibbles = windows >> 4
    low_nibbles = windows & 0x0F
    nibbles = np.concatenate((high_nibbles, low_nibbles), axis=1)

    # 4. Calculate Chi-Square for each window
    # We have 16 bins (0-15).
    # Expected count (E) per bin = total_nibbles / 16
    n_nibbles = nibbles.shape[1] # Should be 128 for window_size=64
    E = n_nibbles / 16.0         # Should be 8.0
    
    # Count occurrences of 0-15 in each window
    # We use an identity matrix trick to vectorize bincount across rows
    # nibbles is (n_windows, 128) -> values 0..15
    # Result counts is (n_windows, 16)
    rows = np.arange(n_windows)[:, None]
    counts = np.zeros((n_windows, 16), dtype=int)
    np.add.at(counts, (rows, nibbles), 1)

    # Compute Chi-Square for each window: sum((O - E)^2 / E)
    # Result is array of shape (n_windows,)
    chi2_scores = np.sum(((counts - E) ** 2) / E, axis=1)

    # 5. Return the Variance of these scores
    return float(np.var(chi2_scores))
