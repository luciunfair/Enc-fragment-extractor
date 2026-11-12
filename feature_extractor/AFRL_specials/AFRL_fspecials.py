import math
import numpy as np
from scipy.special import gammaincc


def _afrl_approximate_entropy_test(byte_data):
    """
    Approximate Entropy Test (NIST SP 800-22) — NumPy Vectorized (Byte Input).
    """
    data = np.asarray(byte_data, dtype=np.uint8)
    bits = np.unpackbits(data)
    n = len(bits)

    m = int(np.floor(np.log2(n))) - 6
    if m < 2:
        m = 2
    if m > 3:
        m = 3

    phi_m = []
    for iterm in range(m, m + 2):
        padded_bits = np.concatenate((bits, bits[:iterm - 1]))
        vals = np.zeros(n, dtype=np.int8)
        for k in range(iterm):
            vals <<= 1
            vals |= padded_bits[k:n + k]

        counts = np.bincount(vals, minlength=2 ** iterm)
        Ci = counts.astype(np.float64) / n
        valid_Ci = Ci[Ci > 0]
        phi_val = np.sum(valid_Ci * np.log(valid_Ci / 10.0))
        phi_m.append(phi_val)

    appen_m = phi_m[0] - phi_m[1]
    chisq = 2 * n * (np.log(2) - appen_m)
    p = gammaincc(2 ** (m - 1), (chisq / 2.0))

    success = (p >= 0.01)
    return (success, float(p), None)


def _afrl_frequency_within_block_test(byte_data):
    """
    Frequency (Monobit) Test within a Block (NIST SP 800-22) — NumPy Vectorized (Byte Input).
    """
    data = np.asarray(byte_data, dtype=np.uint8)
    bits = np.unpackbits(data)
    n = bits.size

    if n < 100:
        return (False, 1.0, None)

    M = 20
    N = n // M
    if N > 99:
        N = 99
        M = n // N

    bits = bits[:N * M]
    blocks = bits.reshape((N, M))
    ones = np.sum(blocks, axis=1)
    proportions = ones / float(M)

    chisq = np.sum(4.0 * M * (proportions - 0.5) ** 2)
    p = gammaincc(N / 2.0, chisq / 2.0)

    success = (p >= 0.01)
    return (success, float(p), None)


def _afrl_runs_test(byte_data):
    """
    Runs Test (NIST SP 800-22) — NumPy Vectorized (Byte Input).
    """
    data = np.asarray(byte_data, dtype=np.uint8)
    bits = np.unpackbits(data)
    n = bits.size

    ones = np.sum(bits)
    prop = ones / float(n)
    tau = 2.0 / math.sqrt(n)

    if abs(prop - 0.5) > tau:
        return (False, 0.0, None)

    transitions = np.sum(bits[:-1] != bits[1:])
    vobs = 1.0 + transitions

    numerator = abs(vobs - (2.0 * n * prop * (1.0 - prop)))
    denominator = 2.0 * math.sqrt(2.0 * n) * prop * (1.0 - prop)
    p = math.erfc(numerator / denominator)

    success = (p >= 0.01)
    return (success, float(p), None)


def local_nibbles_variance(byte_data, window_size=64, threshold=46.98):
    """
    Feature: Local Nibbles Variance (LNV)

    Calculates the variance of the Chi-Square statistic for nibble (4-bit) distributions
    across sliding windows. High variance indicates structure (Compression);
    Low variance indicates uniformity (Encryption).

    Returns:
        (success: bool, variance_stat: float, None)
    """
    data = np.asarray(byte_data, dtype=np.uint8)

    if len(data) < (window_size * 1.5):
        return (False, 0.0, None)

    step = window_size // 2
    n_windows = (len(data) - window_size) // step + 1

    indexer = np.arange(window_size)[None, :] + np.arange(n_windows)[:, None] * step
    windows = data[indexer]

    high_nibbles = windows >> 4
    low_nibbles = windows & 0x0F
    nibbles = np.concatenate((high_nibbles, low_nibbles), axis=1)

    n_nibbles = nibbles.shape[1]
    E = n_nibbles / 16.0

    rows = np.arange(n_windows)[:, None]
    counts = np.zeros((n_windows, 16), dtype=int)
    np.add.at(counts, (rows, nibbles), 1)

    chi2_scores = np.sum(((counts - E) ** 2) / E, axis=1)
    var_val = float(np.var(chi2_scores))
    success = (var_val <= threshold)

    return (success, var_val, None)


def AFRL_test(byte_data):
    """
    AFRL Combined Encryption Detection Classifier.
    Combines:
      1. Approximate Entropy Test (NIST SP 800-22)
      2. Frequency Within Block Test (NIST SP 800-22)
      3. Runs Test (NIST SP 800-22)
      4. Local Nibbles Variance (LNV <= 46.98)

    Returns:
        bool: True (1) = Encrypted, False (0) = Unencrypted
    """
    row = np.asarray(byte_data, dtype=np.uint8)

    r1 = _afrl_approximate_entropy_test(row)[0]
    r2 = _afrl_frequency_within_block_test(row)[0]
    r3 = _afrl_runs_test(row)[0]
    r4 = local_nibbles_variance(row)[0]

    return bool(r1 and r2 and r3 and r4)