import math
import numpy as np
from scipy.special import gammaincc


def _afrl_approximate_entropy_test(byte_data):
    """Approximate Entropy Test (NIST SP 800-22) — NumPy Vectorized (Any Length)."""
    data = np.asarray(byte_data, dtype=np.uint8)
    bits = np.unpackbits(data)
    n = bits.size
    if n < 16:
        return (False, 0.0, None)

    m = int(np.floor(np.log2(n))) - 6
    m = max(2, min(3, m))

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
    chisq = 2.0 * n * (np.log(2.0) - appen_m)
    p = float(gammaincc(2 ** (m - 1), (chisq / 2.0)))

    return (p >= 0.01, p, None)


def _afrl_frequency_within_block_test(byte_data):
    """Frequency Within a Block Test (NIST SP 800-22) — NumPy Vectorized (Any Length)."""
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
    p = float(gammaincc(N / 2.0, chisq / 2.0))

    return (p >= 0.01, p, None)


def _afrl_runs_test(byte_data):
    """Runs Test (NIST SP 800-22) — NumPy Vectorized (Any Length)."""
    data = np.asarray(byte_data, dtype=np.uint8)
    bits = np.unpackbits(data)
    n = bits.size
    if n < 2:
        return (False, 0.0, None)

    ones = int(np.sum(bits))
    prop = ones / float(n)
    tau = 2.0 / math.sqrt(n)

    if abs(prop - 0.5) > tau or prop <= 0.0 or prop >= 1.0:
        return (False, 0.0, None)

    transitions = int(np.sum(bits[:-1] != bits[1:]))
    vobs = 1.0 + transitions

    numerator = abs(vobs - (2.0 * n * prop * (1.0 - prop)))
    denominator = 2.0 * math.sqrt(2.0 * n) * prop * (1.0 - prop)
    p = float(math.erfc(numerator / denominator))

    return (p >= 0.01, p, None)


def local_nibbles_variance(byte_data, window_size=None, base_threshold=46.98):
    """
    Feature: Local Nibbles Variance (LNV) — Adaptive for ANY Fragment Length.
    - Uses standard window_size=64 for all fragments >= 256 bytes, and scales window_size
      proportionally (down to 16 bytes) for smaller fragments so sufficient windows exist.
    - Scales threshold smoothly with the number of sliding windows N_w using finite-sample
      chi-square variance moments (exact match to threshold=46.98 at L=4096 bytes).
    """
    data = np.asarray(byte_data, dtype=np.uint8)
    L = data.size

    if L < 24:
        return (False, 0.0, None)

    if window_size is None:
        if L >= 256:
            w_size = 64
        else:
            w_size = max(16, (L // 4) - ((L // 4) % 2))
    else:
        w_size = int(window_size)

    step = max(1, w_size // 2)
    n_windows = (L - w_size) // step + 1
    if n_windows < 2:
        return (False, 0.0, None)

    indexer = np.arange(w_size)[None, :] + np.arange(n_windows)[:, None] * step
    windows = data[indexer]

    high_nibbles = windows >> 4
    low_nibbles = windows & 0x0F
    nibbles = np.concatenate((high_nibbles, low_nibbles), axis=1)

    n_nibbles = nibbles.shape[1]
    E = n_nibbles / 16.0

    rows = np.arange(n_windows)[:, None]
    counts = np.zeros((n_windows, 16), dtype=np.int32)
    np.add.at(counts, (rows, nibbles), 1)

    chi2_scores = np.sum(((counts - E) ** 2) / E, axis=1)
    var_val = float(np.var(chi2_scores))

    ref_windows = 127.0
    mu_var = 30.0 + (210.0 / float(n_nibbles))
    scale = math.sqrt((ref_windows - 1.0) / max(1.0, float(n_windows - 1)))
    scale = min(scale, 2.2)
    adaptive_threshold = mu_var + (base_threshold - 31.640625) * scale

    success = (var_val <= adaptive_threshold)
    return (success, var_val, None)


def AFRL_test(byte_data):
    """
    AFRL Combined Encryption Detection Classifier (Works on ANY Fragment Length).
    """
    row = np.asarray(byte_data, dtype=np.uint8)

    r1 = _afrl_approximate_entropy_test(row)[0]
    r2 = _afrl_frequency_within_block_test(row)[0]
    r3 = _afrl_runs_test(row)[0]
    r4 = local_nibbles_variance(row)[0]

    return bool(r1 and r2 and r3 and r4)