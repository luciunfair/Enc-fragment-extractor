import math
import zlib
import bz2
import lzma
import numpy as np
from scipy.stats import skew, kurtosis, kstest, wasserstein_distance
from scipy.fft import rfft


def byte_statistical_moments(byte_sequence: np.ndarray):
    """Mean, Variance, Skewness, Excess Kurtosis over U{0..255}."""
    if byte_sequence.size == 0:
        return [0.0, 0.0, 0.0, 0.0]
    arr = byte_sequence.astype(np.float64)
    mean_val = float(np.mean(arr))
    var_val = float(np.var(arr))
    if var_val > 1e-12:
        return [mean_val, var_val, float(skew(arr)), float(kurtosis(arr))]
    return [mean_val, var_val, 0.0, 0.0]


def ks_uniformity_test(byte_sequence: np.ndarray):
    """Two-sided Kolmogorov-Smirnov goodness-of-fit test against U[0, 1]."""
    if byte_sequence.size == 0:
        return [1.0, 0.0]
    stat, pval = kstest(byte_sequence.astype(np.float64) / 255.0, 'uniform')
    return [float(stat), float(pval)]


def wasserstein_uniform_distance(byte_sequence: np.ndarray):
    """1-Wasserstein (Earth Mover's) Distance to ideal uniform distribution over [0, 255]."""
    n = byte_sequence.size
    if n == 0:
        return 0.0
    return float(wasserstein_distance(byte_sequence.astype(np.float64), np.linspace(0.0, 255.0, n)))


def index_of_coincidence(byte_sequence: np.ndarray):
    """Unbiased Index of Coincidence (IoC): ~1/256 (0.00390625) for uniform random."""
    n = byte_sequence.size
    if n < 2:
        return 0.0
    counts = np.bincount(byte_sequence, minlength=256).astype(np.float64)
    return float(np.sum(counts * (counts - 1.0)) / (n * (n - 1.0)))


def generalized_entropies(byte_sequence: np.ndarray):
    """Rényi Collision Entropy (H2), Min-Entropy (H_inf), and Tsallis Entropy (q=2)."""
    n = byte_sequence.size
    if n == 0:
        return [0.0, 0.0, 0.0]
    counts = np.bincount(byte_sequence, minlength=256)
    probs = counts[counts > 0].astype(np.float64) / float(n)
    sum_p2 = float(np.sum(probs ** 2))
    h_collision = float(-np.log2(sum_p2)) if sum_p2 > 0.0 else 0.0
    h_min = float(-np.log2(np.max(probs)))
    h_tsallis = float(1.0 - sum_p2)
    return [h_collision, h_min, h_tsallis]


def serial_and_lag_autocorrelation(byte_sequence: np.ndarray):
    """Circular Serial Correlation (lag-1) and normalized autocorrelations at lags {2, 4, 8}."""
    n = byte_sequence.size
    if n < 10:
        return [0.0, 0.0, 0.0, 0.0]
    arr = byte_sequence.astype(np.float64)
    mean_v = float(np.mean(arr))
    xc = arr - mean_v
    denom = float(np.sum(xc ** 2))
    if denom <= 1e-12:
        return [0.0, 0.0, 0.0, 0.0]

    scc = float((np.sum(xc[:-1] * xc[1:]) + xc[-1] * xc[0]) / denom)
    return [
        scc,
        float(np.sum(xc[:-2] * xc[2:]) / denom),
        float(np.sum(xc[:-4] * xc[4:]) / denom),
        float(np.sum(xc[:-8] * xc[8:]) / denom),
    ]


def markov_bigram_features(byte_sequence: np.ndarray):
    """Computes 4 second-order (1st-order Markov) byte transition metrics."""
    n = byte_sequence.size
    if n < 2:
        return [0.0, 0.0, 0.0, 0.0]

    u_counts = np.bincount(byte_sequence, minlength=256)
    u_probs = u_counts[u_counts > 0].astype(np.float64) / float(n)
    h_uni = float(-np.sum(u_probs * np.log2(u_probs)))

    num_bigrams = n - 1
    bigrams = (byte_sequence[:-1].astype(np.uint32) << 8) | byte_sequence[1:].astype(np.uint32)
    b_counts = np.bincount(bigrams, minlength=65536)
    nz_b = b_counts[b_counts > 0].astype(np.float64)
    b_probs = nz_b / float(num_bigrams)
    h_bi = float(-np.sum(b_probs * np.log2(b_probs)))

    h_cond = max(0.0, h_bi - h_uni)
    mi = max(0.0, h_uni - h_cond)
    expected_unique = 65536.0 * (1.0 - ((65535.0 / 65536.0) ** num_bigrams))
    unique_ratio = float(nz_b.size / max(1.0, expected_unique))
    return [h_bi, h_cond, mi, unique_ratio]


def byte_difference_features(byte_sequence: np.ndarray):
    """Modulo-256 byte difference entropy and mean absolute consecutive byte jump."""
    n = byte_sequence.size
    if n < 2:
        return [0.0, 0.0]
    mod_diff = np.diff(byte_sequence)
    d_counts = np.bincount(mod_diff, minlength=256)
    d_probs = d_counts[d_counts > 0].astype(np.float64) / float(mod_diff.size)
    h_diff = float(-np.sum(d_probs * np.log2(d_probs)))
    abs_diff_mean = float(np.mean(np.abs(np.diff(byte_sequence.astype(np.float64)))))
    return [h_diff, abs_diff_mean]


def recompression_ratios(byte_sequence: np.ndarray):
    """Lossless re-compression ratios (Compressed Size / Original Size) via zlib, bz2, lzma."""
    n = byte_sequence.size
    if n == 0:
        return [0.0, 0.0, 0.0]
    raw = byte_sequence.tobytes()
    inv_n = 1.0 / float(n)
    return [
        float(len(zlib.compress(raw, level=6)) * inv_n),
        float(len(bz2.compress(raw, compresslevel=9)) * inv_n),
        float(len(lzma.compress(raw)) * inv_n),
    ]


def haar_wavelet_detail_ratio(byte_sequence: np.ndarray):
    """1-level Haar DWT detail energy ratio on zero-mean signal (~0.50 for white noise)."""
    n = byte_sequence.size
    if n < 2:
        return 0.0
    even_len = n & ~1
    arr_centered = byte_sequence[:even_len].astype(np.float64)
    arr_centered -= np.mean(arr_centered)
    even = arr_centered[0::2]
    odd = arr_centered[1::2]
    e_approx = float(np.sum((even + odd) ** 2))
    e_detail = float(np.sum((even - odd) ** 2))
    total_e = e_approx + e_detail
    return float(e_detail / total_e) if total_e > 1e-12 else 0.0


def spectral_flatness_measure(byte_sequence: np.ndarray):
    """Wiener Entropy (Spectral Flatness Measure) for ANY fragment length L >= 4."""
    n = byte_sequence.size
    if n < 4:
        return 0.0
    arr_centered = byte_sequence.astype(np.float64)
    arr_centered -= np.mean(arr_centered)
    power_spec = (np.abs(rfft(arr_centered))[1:]) ** 2
    arith_mean = float(np.mean(power_spec)) if power_spec.size else 0.0
    if arith_mean <= 1e-12:
        return 0.0
    power_spec = np.maximum(power_spec, 1e-12)
    return float(np.exp(np.mean(np.log(power_spec))) / arith_mean)


def structural_run_and_ascii(byte_sequence: np.ndarray):
    """Length-normalized max byte run & printable ASCII ratio (0x20..0x7E)."""
    n = byte_sequence.size
    if n == 0:
        return [0.0, 0.0]
    change_idx = np.flatnonzero(np.diff(byte_sequence) != 0)
    if change_idx.size == 0:
        max_run = float(n)
    else:
        boundaries = np.empty(change_idx.size + 2, dtype=np.int64)
        boundaries[0] = -1
        boundaries[1:-1] = change_idx
        boundaries[-1] = n - 1
        max_run = float(np.max(np.diff(boundaries)))

    expected_max_run = max(1.0, (math.log(max(2, n)) / math.log(256.0)) + 1.0)
    ascii_ratio = float(np.mean((byte_sequence >= 32) & (byte_sequence <= 126)))
    return [float(max_run / expected_max_run), ascii_ratio]


def SOTA_test(byte_data) -> int:
    """
    SOTA 2024-2026 Multi-Modal Statistical Hypothesis Detector (1 = Encrypted, 0 = Unencrypted).
    Ordered from O(L) byte checks to spectral/recompression checks for rapid rejection.
    """
    row = np.asarray(byte_data, dtype=np.uint8)
    L = row.size
    if L < 64:
        return 0
    inv_sqrt_L = 1.0 / math.sqrt(L)

    p_ascii = 95.0 / 256.0
    ascii_ratio = float(np.mean((row >= 32) & (row <= 126)))
    if abs(ascii_ratio - p_ascii) > 3.5 * math.sqrt(p_ascii * (1.0 - p_ascii) / float(L)):
        return 0

    counts = np.bincount(row, minlength=256).astype(np.float64)
    ioc = float(np.sum(counts * (counts - 1.0)) / (L * (L - 1.0)))
    chi2_equiv = (L - 1.0) * (256.0 * ioc - 1.0) + 255.0
    if abs(chi2_equiv - 255.0) > 2.65 * 22.58318:
        return 0

    max_prob = float(np.max(counts)) / float(L)
    h_min = -math.log2(max_prob) if max_prob > 0.0 else 0.0
    mu_bin = L / 256.0
    sigma_bin = math.sqrt(L * (1.0 / 256.0) * (255.0 / 256.0))
    min_allowed_hmin = -math.log2(min(0.99, (mu_bin + 4.8 * sigma_bin) / float(L)))
    if h_min < min_allowed_hmin:
        return 0

    if max(abs(v) for v in serial_and_lag_autocorrelation(row)) > (3.2 * inv_sqrt_L):
        return 0

    if ks_uniformity_test(row)[1] < 0.01:
        return 0
    if wasserstein_uniform_distance(row) > (235.0 * inv_sqrt_L):
        return 0

    sfm_min = 0.44 if L < 1024 else 0.50
    if spectral_flatness_measure(row) < sfm_min:
        return 0
    if abs(haar_wavelet_detail_ratio(row) - 0.50) > (2.2 * inv_sqrt_L):
        return 0

    _, _, _, unique_bigram_ratio = markov_bigram_features(row)
    if abs(unique_bigram_ratio - 1.0) > max(0.03, 2.5 * inv_sqrt_L):
        return 0

    raw = row.tobytes()
    inv_L = 1.0 / float(L)
    if len(zlib.compress(raw, level=6)) * inv_L < 0.995:
        return 0
    if len(bz2.compress(raw, compresslevel=9)) * inv_L < 0.995:
        return 0
    if len(lzma.compress(raw)) * inv_L < 0.995:
        return 0

    return 1