import math
import numpy as np
from scipy.special import gammaincc
from scipy.stats import chi2


def _hedge_approximate_entropy_from_bits(bits: np.ndarray):
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


def _hedge_frequency_within_block_from_bits(bits: np.ndarray):
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


def _cusum_p_value(n: int, z: int) -> float:
    if z <= 0:
        return 1.0
    inv_sqrt_n = 1.0 / math.sqrt(n)
    inv_z = n / float(z)

    k1_start = int(math.floor((-inv_z + 1.0) * 0.25))
    k1_end = int(math.floor((inv_z - 1.0) * 0.25))
    if k1_end >= k1_start:
        k1 = np.arange(k1_start, k1_end + 1, dtype=np.float64)
        c1 = ((4.0 * k1 + 1.0) * z) * inv_sqrt_n
        c2 = ((4.0 * k1 - 1.0) * z) * inv_sqrt_n
        sum_a = float(np.sum(0.5 * (np.vectorize(math.erfc)(-c1 * math.sqrt(0.5)) - np.vectorize(math.erfc)(-c2 * math.sqrt(0.5)))))
    else:
        sum_a = 0.0

    k2_start = int(math.floor((-inv_z - 3.0) * 0.25))
    k2_end = int(math.floor((inv_z - 1.0) * 0.25))
    if k2_end >= k2_start:
        k2 = np.arange(k2_start, k2_end + 1, dtype=np.float64)
        c1 = ((4.0 * k2 + 3.0) * z) * inv_sqrt_n
        c2 = ((4.0 * k2 + 1.0) * z) * inv_sqrt_n
        sum_b = float(np.sum(0.5 * (np.vectorize(math.erfc)(-c1 * math.sqrt(0.5)) - np.vectorize(math.erfc)(-c2 * math.sqrt(0.5)))))
    else:
        sum_b = 0.0

    return float(1.0 - sum_a + sum_b)


def _hedge_cumulative_sums_from_bits(bits: np.ndarray):
    n = bits.size
    if n < 2:
        return (False, [0.0, 0.0])
    x = 2 * bits.astype(np.int32) - 1
    cs_fwd = np.cumsum(x)
    fwd_max = int(np.max(np.abs(cs_fwd)))
    total_sum = int(cs_fwd[-1])
    bwd_max = max(abs(total_sum), int(np.max(np.abs(total_sum - cs_fwd[:-1]))))

    p_fwd = _cusum_p_value(n, fwd_max)
    if p_fwd < 0.01:
        return (False, [p_fwd, 0.0])
    p_bwd = _cusum_p_value(n, bwd_max)
    return (p_bwd >= 0.01, [p_fwd, p_bwd])


def chi2_uniform(row, Length=None):
    arr = np.asarray(row, dtype=np.uint8)
    L = arr.size if Length is None else int(Length)
    if L <= 0:
        return 0.0, 0.0
    Ei = L / 256.0
    observed = np.bincount(arr, minlength=256).astype(np.float64)
    chi2_stat = float(np.sum(((observed - Ei) ** 2) / Ei))
    p_value = float(chi2.sf(chi2_stat, df=255))
    return chi2_stat, p_value


def chi_test_absolute(chi2_uniform_stat, gamma=2, length=4096):
    L = max(1, int(length))
    hedge_table = {
        1024:  (255.02, 22.57),
        2048:  (254.98, 22.57),
        4096:  (255.04, 22.60),
        8192:  (255.09, 22.54),
        16384: (254.96, 22.76),
        32768: (255.08, 22.68),
        65536: (255.37, 22.82),
    }
    if L in hedge_table:
        mu, sigma = hedge_table[L]
    else:
        mu = 255.0
        sigma = math.sqrt(510.0 + (64770.0 / float(L)))
    return 1 if ((mu - gamma * sigma) <= chi2_uniform_stat <= (mu + gamma * sigma)) else 0


def chi_test_confidence(chi2_uniform_pvalue):
    return 0 if (chi2_uniform_pvalue < 0.01 or chi2_uniform_pvalue > 0.99) else 1


def HEDGE_test(byte_data) -> int:
    """
    HEDGE Encrypted Fragment Detection Method (Fast chi2 pre-check + single bit unpack).
    Returns: 1 = Encrypted fragment, 0 = Unencrypted fragment
    """
    row = np.asarray(byte_data, dtype=np.uint8)
    frag_len = row.size
    if frag_len == 0:
        return 0

    chi2_stat, p_val = chi2_uniform(row, Length=frag_len)
    if chi_test_absolute(chi2_stat, gamma=2, length=frag_len) != 1:
        return 0
    if chi_test_confidence(p_val) != 1:
        return 0

    bits = np.unpackbits(row)
    if not _hedge_frequency_within_block_from_bits(bits)[0]:
        return 0
    if not _hedge_cumulative_sums_from_bits(bits)[0]:
        return 0
    if not _hedge_approximate_entropy_from_bits(bits)[0]:
        return 0
    return 1