import math
import numpy as np
from scipy.fft import rfft


def byte_frequency_distribution(byte_sequence):
    """Calculates normalized Byte Frequency Distribution (256 values) for ANY fragment length."""
    arr = np.asarray(byte_sequence, dtype=np.uint8)
    n = arr.size
    if n == 0:
        return np.zeros(256, dtype=np.float32)
    counts = np.bincount(arr, minlength=256).astype(np.float32)
    return counts / float(n)


def discrete_fourier_transform(byte_sequence, num_coeffs=5):
    """
    Calculates DFT features: 'num_coeffs' highest magnitude coefficients and their
    length-normalized frequency indices (2 * num_coeffs = 10 values) for ANY fragment length.
    """
    arr = np.asarray(byte_sequence, dtype=np.float64)
    sequence_len = arr.size
    if sequence_len < 2:
        return np.zeros(2 * num_coeffs, dtype=np.float32)

    magnitudes = np.abs(rfft(arr))
    if magnitudes.size == 0:
        return np.zeros(2 * num_coeffs, dtype=np.float32)

    actual_k = min(num_coeffs, magnitudes.size)
    top_indices = np.argsort(magnitudes)[-actual_k:]
    top_mags = magnitudes[top_indices]

    dft_feats = np.zeros(2 * num_coeffs, dtype=np.float32)
    total_mag = float(np.sum(magnitudes))
    top_mags_norm = (top_mags / total_mag) if total_mag > 0.0 else top_mags

    half_len = max(1, sequence_len // 2)
    dft_feats[:actual_k] = top_mags_norm
    dft_feats[num_coeffs:num_coeffs + actual_k] = top_indices.astype(np.float32) / float(half_len)
    return dft_feats


def lempel_ziv_complexity(byte_sequence_np):
    """
    Calculates Length-Normalized Lempel-Ziv Complexity (Kaspar-Schuster LZ76).
    Uses exact byte keys and normalizes by (L / log_256(L)) so output is ~1.0
    for random ciphertext of ANY length L.
    """
    arr = np.asarray(byte_sequence_np, dtype=np.uint8)
    n = arr.size
    if n == 0:
        return 0.0
    if n == 1:
        return 1.0

    raw = arr.tobytes()
    dictionary = set()
    w = b""
    complexity = 0

    for i in range(n):
        c = raw[i:i + 1]
        wc = w + c
        if wc in dictionary:
            w = wc
        else:
            dictionary.add(wc)
            w = c
            complexity += 1
            if w not in dictionary:
                dictionary.add(w)

    log256_n = math.log(n) / math.log(256.0)
    norm_factor = n / max(1e-9, log256_n)
    return float(complexity / norm_factor)


def monte_carlo_pi_approximation(byte_sequence):
    """Calculates vectorized Monte Carlo Pi approximation (~3.14159) for ANY fragment length."""
    arr = np.asarray(byte_sequence, dtype=np.uint8)
    num_pairs = arr.size // 2
    if num_pairs == 0:
        return 0.0

    x = arr[0:2 * num_pairs:2].astype(np.float64) / 255.0
    y = arr[1:2 * num_pairs:2].astype(np.float64) / 255.0
    in_circle = np.sum(((x - 0.5) ** 2 + (y - 0.5) ** 2) <= 0.25)
    return float(4.0 * in_circle / float(num_pairs))