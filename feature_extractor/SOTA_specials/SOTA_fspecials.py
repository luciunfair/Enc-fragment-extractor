import math
import zlib
import bz2
import lzma
import numpy as np
from scipy.stats import skew, kurtosis, kstest, wasserstein_distance
from scipy.fft import rfft


# =====================================================================
# CATEGORY A: HIGHER-ORDER MOMENTS & GOODNESS-OF-FIT TO UNIFORMITY
# =====================================================================
def byte_statistical_moments(byte_sequence: np.ndarray):
    """
    Computes 4 distributional moments: Mean, Variance, Skewness, Excess Kurtosis.
    Theoretical U{0..255}: Mean=127.5, Var=5461.25, Skew=0.0, Kurtosis=-1.2 (any length L).
    """
    if byte_sequence.size == 0:
        return [0.0, 0.0, 0.0, 0.0]
    arr = byte_sequence.astype(np.float64)
    mean_val = float(np.mean(arr))
    var_val = float(np.var(arr))
    if var_val > 1e-12:
        skew_val = float(skew(arr))
        kurt_val = float(kurtosis(arr))
    else:
        skew_val = 0.0
        kurt_val = 0.0
    return [mean_val, var_val, skew_val, kurt_val]


def ks_uniformity_test(byte_sequence: np.ndarray):
    """Two-sided Kolmogorov-Smirnov goodness-of-fit test against U[0, 1] for ANY length L."""
    if byte_sequence.size == 0:
        return [1.0, 0.0]
    arr_norm = byte_sequence.astype(np.float64) / 255.0
    stat, pval = kstest(arr_norm, 'uniform')
    return [float(stat), float(pval)]


def wasserstein_uniform_distance(byte_sequence: np.ndarray):
    """1-Wasserstein (Earth Mover's) Distance to ideal uniform distribution over [0, 255]."""
    n = byte_sequence.size
    if n == 0:
        return 0.0
    arr = byte_sequence.astype(np.float64)
    uniform_ref = np.linspace(0.0, 255.0, n)
    return float(wasserstein_distance(arr, uniform_ref))


# =====================================================================
# CATEGORY B: INDEX OF COINCIDENCE & GENERALIZED ENTROPIES
# =====================================================================
def index_of_coincidence(byte_sequence: np.ndarray):
    """Unbiased Index of Coincidence (IoC): ~1/256 (0.00390625) for ANY length L >= 2."""
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
    probs = counts[counts > 0].astype(np.float64) / n
    sum_p2 = float(np.sum(probs ** 2))
    h_collision = float(-np.log2(sum_p2)) if sum_p2 > 0 else 0.0
    h_min = float(-np.log2(np.max(probs)))
    h_tsallis = float(1.0 - sum_p2)
    return [h_collision, h_min, h_tsallis]


# =====================================================================
# CATEGORY C: SEQUENTIAL DEPENDENCE, AUTOCORRELATION & MARKOV BIGRAMS
# =====================================================================
def serial_and_lag_autocorrelation(byte_sequence: np.ndarray):
    """Circular Serial Correlation (lag-1) and normalized autocorrelations at lags {2, 4, 8}."""
    n = byte_sequence.size
    if n < 10:
        return [0.0, 0.0, 0.0, 0.0]
    arr = byte_sequence.astype(np.float64)

    x_next = np.roll(arr, -1)
    sum_x = np.sum(arr)
    num = n * np.sum(arr * x_next) - (sum_x ** 2)
    den = n * np.sum(arr ** 2) - (sum_x ** 2)
    scc = float(num / den) if abs(den) > 1e-12 else 0.0

    x_centered = arr - np.mean(arr)
    denom_ac = float(np.sum(x_centered ** 2))
    lags_out = [scc]
    for lag in (2, 4, 8):
        if denom_ac > 1e-12 and n > lag:
            ac = float(np.sum(x_centered[:-lag] * x_centered[lag:]) / denom_ac)
        else:
            ac = 0.0
        lags_out.append(ac)
    return lags_out


def markov_bigram_features(byte_sequence: np.ndarray):
    """
    Computes 4 scale-calibrated second-order (1st-order Markov) byte transition metrics:
      1. Bigram Joint Shannon Entropy H(X_t, X_{t+1})
      2. Conditional Entropy H(X_{t+1} | X_t)
      3. Mutual Information I(X_t; X_{t+1})
      4. Birthday-Normalized Unique Bigram Ratio (~1.0 for uniform random at ANY length L)
    """
    n = byte_sequence.size
    if n < 2:
        return [0.0, 0.0, 0.0, 0.0]

    u_counts = np.bincount(byte_sequence, minlength=256)
    u_probs = u_counts[u_counts > 0].astype(np.float64) / n
    h_uni = float(-np.sum(u_probs * np.log2(u_probs)))

    num_bigrams = n - 1
    bigrams = (byte_sequence[:-1].astype(np.uint32) << 8) | byte_sequence[1:].astype(np.uint32)
    b_counts = np.bincount(bigrams, minlength=65536)
    nz_b = b_counts[b_counts > 0].astype(np.float64)
    b_probs = nz_b / float(num_bigrams)
    h_bi = float(-np.sum(b_probs * np.log2(b_probs)))

    h_cond = max(0.0, h_bi - h_uni)
    mi = max(0.0, h_uni - h_cond)

    # Exact birthday-problem expected distinct bigrams under uniform random:
    # E[unique] = 65536 * (1 - (1 - 1/65536)^(n-1))
    expected_unique = 65536.0 * (1.0 - ((65535.0 / 65536.0) ** num_bigrams))
    unique_ratio = float(nz_b.size / max(1.0, expected_unique))
    return [h_bi, h_cond, mi, unique_ratio]


def byte_difference_features(byte_sequence: np.ndarray):
    """Modulo-256 byte difference entropy and mean absolute consecutive byte jump."""
    n = byte_sequence.size
    if n < 2:
        return [0.0, 0.0]
    mod_diff = np.diff(byte_sequence).astype(np.uint8)
    d_counts = np.bincount(mod_diff, minlength=256)
    d_probs = d_counts[d_counts > 0].astype(np.float64) / mod_diff.size
    h_diff = float(-np.sum(d_probs * np.log2(d_probs)))

    abs_diff_mean = float(np.mean(np.abs(np.diff(byte_sequence.astype(np.float64)))))
    return [h_diff, abs_diff_mean]


# =====================================================================
# CATEGORY D: KOLMOGOROV COMPLEXITY PROXIES (RE-COMPRESSION RATIOS)
# =====================================================================
def recompression_ratios(byte_sequence: np.ndarray):
    """Lossless re-compression ratios (Compressed Size / Original Size) via zlib, bz2, lzma."""
    n = byte_sequence.size
    if n == 0:
        return [0.0, 0.0, 0.0]
    raw = byte_sequence.tobytes()
    r_zlib = float(len(zlib.compress(raw, level=6)) / float(n))
    r_bz2 = float(len(bz2.compress(raw, compresslevel=9)) / float(n))
    r_lzma = float(len(lzma.compress(raw)) / float(n))
    return [r_zlib, r_bz2, r_lzma]


# =====================================================================
# CATEGORY E: WAVELET, SPECTRAL FLATNESS & STRUCTURAL EVASION METRICS
# =====================================================================
def haar_wavelet_detail_ratio(byte_sequence: np.ndarray):
    """1-level Haar DWT detail energy ratio on zero-mean signal (~0.50 for white noise)."""
    n = byte_sequence.size
    if n < 2:
        return 0.0
    even_len = n - (n % 2)
    arr = byte_sequence[:even_len].astype(np.float64)
    arr_centered = arr - np.mean(arr)
    even = arr_centered[0::2]
    odd = arr_centered[1::2]
    approx = (even + odd) * 0.7071067811865475
    detail = (even - odd) * 0.7071067811865475
    e_approx = float(np.sum(approx ** 2))
    e_detail = float(np.sum(detail ** 2))
    total_e = e_approx + e_detail
    return float(e_detail / total_e) if total_e > 1e-12 else 0.0


def spectral_flatness_measure(byte_sequence: np.ndarray):
    """Wiener Entropy (Spectral Flatness Measure) for ANY fragment length L >= 4."""
    n = byte_sequence.size
    if n < 4:
        return 0.0
    arr = byte_sequence.astype(np.float64)
    arr_centered = arr - np.mean(arr)
    power_spec = (np.abs(rfft(arr_centered))[1:]) ** 2
    if power_spec.size == 0 or np.mean(power_spec) <= 1e-12:
        return 0.0
    power_spec = np.maximum(power_spec, 1e-12)
    geo_mean = float(np.exp(np.mean(np.log(power_spec))))
    arith_mean = float(np.mean(power_spec))
    return float(geo_mean / arith_mean)


def structural_run_and_ascii(byte_sequence: np.ndarray):
    """
    Computes 2 length-normalized structural/evasion metrics:
      1. Length-Normalized Max Consecutive Identical Byte Run (divided by expected log_256(n) + 1)
      2. Printable ASCII byte ratio (0x20..0x7E) - detects Base64/Hex ciphertext evasion
    """
    n = byte_sequence.size
    if n == 0:
        return [0.0, 0.0]
    diffs = np.diff(byte_sequence)
    change_idx = np.flatnonzero(diffs != 0)
    if change_idx.size == 0:
        max_run = float(n)
    else:
        boundaries = np.concatenate(([-1], change_idx, [n - 1]))
        max_run = float(np.max(np.diff(boundaries)))

    expected_max_run = max(1.0, (math.log(max(2, n)) / math.log(256.0)) + 1.0)
    norm_max_run = float(max_run / expected_max_run)
    ascii_ratio = float(np.mean((byte_sequence >= 32) & (byte_sequence <= 126)))
    return [norm_max_run, ascii_ratio]