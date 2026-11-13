import math
import numpy as np
from scipy.special import gammaincc


def _afrl_approximate_entropy_from_bits(bits: np.ndarray):
    """Approximate Entropy Test (NIST SP 800-22) on pre-unpacked bit array."""
    n = bits.size
    if n < 16:
        return (False, 0.0)

    m = max(2, min(3, int(np.floor(np.log2(n))) - 6))
    phi_m = []
    for iterm in (m, m + 1):
        padded = np.concatenate((bits, bits[:iterm - 1]))
        vals = np.zeros(n, dtype=np.int8)
        for k in range(iterm):
            vals <<= 1
            vals |= padded[k:n + k]
        counts = np.bincount(vals, minlength=1 << iterm)
        ci = counts[counts > 0].astype(np.float64) / float(n)
        phi_m.append(float(np.sum(ci * np.log(ci / 10.0))))

    appen_m = phi_m[0] - phi_m[1]
    chisq = 2.0 * n * (math.log(2.0) - appen_m)
    p = float(gammaincc(1 << (m - 1), chisq / 2.0))
    return (p >= 0.01, p)


def _afrl_frequency_within_block_from_bits(bits: np.ndarray):
    """Frequency Within Block Test (NIST SP 800-22) on pre-unpacked bit array."""
    n = bits.size
    if n < 100:
        return (False, 1.0)

    M = 20
    N = n // M
    if N > 99:
        N = 99
        M = n // N

    blocks = bits[:N * M].reshape((N, M))
    proportions = np.sum(blocks, axis=1, dtype=np.int64) / float(M)
    chisq = float(np.sum(4.0 * M * ((proportions - 0.5) ** 2)))
    p = float(gammaincc(N / 2.0, chisq / 2.0))
    return (p >= 0.01, p)


def _afrl_runs_from_bits(bits: np.ndarray):
    """Runs Test (NIST SP 800-22) on pre-unpacked bit array."""
    n = bits.size
    if n < 2:
        return (False, 0.0)

    prop = float(np.sum(bits, dtype=np.int64)) / float(n)
    if abs(prop - 0.5) > (2.0 / math.sqrt(n)) or prop <= 0.0 or prop >= 1.0:
        return (False, 0.0)

    vobs = 1.0 + float(np.count_nonzero(bits[:-1] != bits[1:]))
    denom = 2.0 * math.sqrt(2.0 * n) * prop * (1.0 - prop)
    p = math.erfc(abs(vobs - (2.0 * n * prop * (1.0 - prop))) / denom)
    return (p >= 0.01, p)


def local_nibbles_variance(byte_data, window_size=64, threshold=46.98):
    """
    Local Nibbles Variance (LNV):
    Computes variance of 4-bit nibble Chi-Square scores across sliding windows
    using zero-copy stride tricks + vectorized row bincounting.
    Returns: (success: bool, variance_stat: float, None)
    """
    data = np.asarray(byte_data, dtype=np.uint8)
    n_bytes = data.size
    if n_bytes < int(window_size * 1.5):
        return (False, 0.0, None)

    step = window_size // 2
    n_windows = (n_bytes - window_size) // step + 1
    stride = data.strides[0]
    windows = np.lib.stride_tricks.as_strided(
        data, shape=(n_windows, window_size), strides=(stride * step, stride)
    )

    nibbles = np.concatenate((windows >> 4, windows & 0x0F), axis=1)
    expected = nibbles.shape[1] / 16.0

    row_offsets = (np.arange(n_windows, dtype=np.int32) * 16)[:, None]
    counts = np.bincount((nibbles.astype(np.int32) + row_offsets).ravel(), minlength=n_windows * 16).reshape(n_windows, 16)

    chi2_scores = np.sum(((counts - expected) ** 2) / expected, axis=1)
    var_val = float(np.var(chi2_scores))
    return (bool(var_val <= threshold), var_val, None)


def AFRL_test(byte_data) -> int:
    """
    AFRL Encrypted Fragment Detection Method (Single-pass bit unpack + fast LNV).
    Returns: 1 = Encrypted fragment, 0 = Unencrypted fragment
    """
    row = np.asarray(byte_data, dtype=np.uint8)
    if row.size < 96:
        return 0

    if not local_nibbles_variance(row)[0]:
        return 0

    bits = np.unpackbits(row)
    if not _afrl_runs_from_bits(bits)[0]:
        return 0
    if not _afrl_frequency_within_block_from_bits(bits)[0]:
        return 0
    if not _afrl_approximate_entropy_from_bits(bits)[0]:
        return 0
    return 1