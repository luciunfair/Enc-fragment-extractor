import math
import numpy as np
from scipy.special import gammaincc, erfc

_INV_SQRT2 = math.sqrt(0.5)
_SQRT2 = math.sqrt(2.0)
_LN2 = math.log(2.0)

_MAURER_EV = (
    0.0, 0.73264948, 1.5374383, 2.40160681, 3.31122472,
    4.25342659, 5.2177052, 6.1962507, 7.1836656,
    8.1764248, 9.1723243, 10.170032, 11.168765,
    12.168070, 13.167693, 14.167488, 15.167379,
)
_MAURER_VAR = (
    0.0, 0.690, 1.338, 1.901, 2.358, 2.705, 2.954, 3.125,
    3.238, 3.311, 3.356, 3.384, 3.401, 3.410, 3.416,
    3.419, 3.421,
)
_EXCURSION_PIXK = np.array([
    [0.5,    0.25,   0.125,  0.0625, 0.0312, 0.0312],
    [0.75,   0.0625, 0.0469, 0.0352, 0.0264, 0.0791],
    [0.8333, 0.0278, 0.0231, 0.0193, 0.0161, 0.0804],
    [0.875,  0.0156, 0.0137, 0.0120, 0.0105, 0.0733],
    [0.9,    0.01,   0.009,  0.0081, 0.0073, 0.0656],
    [0.9167, 0.0069, 0.0064, 0.0058, 0.0053, 0.0588],
    [0.9286, 0.0051, 0.0047, 0.0044, 0.0041, 0.0531],
], dtype=np.float64)
_LC_PI = np.array([0.010417, 0.03125, 0.125, 0.5, 0.25, 0.0625, 0.020833], dtype=np.float64)


def _as_bits(bits) -> np.ndarray:
    if isinstance(bits, np.ndarray) and bits.dtype == np.uint8 and bits.ndim == 1:
        return bits
    return np.asarray(bits, dtype=np.uint8).ravel()


# 1. Longest Run of Ones in a Block Test (2D Vectorized Across All Blocks)
def longest_run_ones_in_a_block_test(bits):
    b = _as_bits(bits)
    n = b.size
    if n < 128:
        return (False, 1.0, None)
    if n < 6272:
        M, K, N = 8, 3, 16
        pi = np.array([0.2148, 0.3672, 0.2305, 0.1875], dtype=np.float64)
    elif n < 750000:
        M, K, N = 128, 5, 49
        pi = np.array([0.1174, 0.2430, 0.2493, 0.1752, 0.1027, 0.1124], dtype=np.float64)
    else:
        M, K, N = 10000, 6, 75
        pi = np.array([0.0882, 0.2092, 0.2483, 0.1933, 0.1208, 0.0675, 0.0727], dtype=np.float64)

    blocks = b[:N * M].reshape((N, M))
    padded = np.zeros((N, M + 2), dtype=np.int8)
    padded[:, 1:-1] = blocks
    diffs = np.diff(padded, axis=1)
    r_idx, c_starts = np.nonzero(diffs == 1)
    _, c_ends = np.nonzero(diffs == -1)

    longest_per_block = np.zeros(N, dtype=np.int32)
    if r_idx.size > 0:
        np.maximum.at(longest_per_block, r_idx, c_ends - c_starts)

    if M == 8:
        bins = np.clip(longest_per_block - 1, 0, 3)
    elif M == 128:
        bins = np.clip(longest_per_block - 4, 0, 5)
    else:
        bins = np.clip(longest_per_block - 10, 0, 6)

    v = np.bincount(bins, minlength=K + 1).astype(np.float64)
    expected = N * pi
    chi_sq = float(np.sum(((v - expected) ** 2) / expected))
    p = float(gammaincc(K / 2.0, chi_sq / 2.0))
    return (p >= 0.01, p, None)


# 2. Overlapping Template Matching Test (Length-Adaptive & Vectorized)
def overlapping_template_matching_test(bits, blen=6):
    b = _as_bits(bits)
    n = b.size
    if n < 512:
        return (False, 0.0, None)

    if n >= 1028016:
        m = 10
        M = 1062
        N = n // M
    else:
        m = max(4, min(9, int(math.floor(math.log2(n / 32.0))) - 1))
        M = max(32, 1 << (m + 1))
        N = n // M
        if N < 16:
            N = 16
            M = n // N
        if M <= m + 1:
            return (False, 0.0, None)

    K = 5
    blocks = b[:N * M].reshape((N, M))
    cs = np.empty((N, M + 1), dtype=np.int32)
    cs[:, 0] = 0
    np.cumsum(blocks, axis=1, out=cs[:, 1:])
    win_sums = cs[:, m:] - cs[:, :-m]
    counts = np.sum(win_sums == m, axis=1)
    v = np.bincount(np.minimum(counts, K), minlength=K + 1).astype(np.float64)

    lambd = (M - m + 1.0) / float(1 << m)
    eta = lambd * 0.5
    log_eta = math.log(eta)

    pi = np.zeros(K + 1, dtype=np.float64)
    pi[0] = math.exp(-eta)
    for u in range(1, K):
        s = 0.0
        for l in range(1, u + 1):
            log_term = (
                -eta - u * _LN2 + l * log_eta
                - math.lgamma(l + 1) + math.lgamma(u) - math.lgamma(l) - math.lgamma(u - l + 1)
            )
            s += math.exp(log_term)
        pi[u] = s
    pi[K] = max(1e-12, 1.0 - float(np.sum(pi[:K])))

    expected = N * pi
    chisq = float(np.sum(((v - expected) ** 2) / np.maximum(expected, 1e-12)))
    p = float(gammaincc(K / 2.0, chisq / 2.0))
    return (p >= 0.01, p, None)


# 3. Approximate Entropy Test (Vectorized)
def approximate_entropy_test(bits):
    b = _as_bits(bits)
    n = b.size
    if n < 16:
        return (False, 0.0, None)

    m = max(2, min(3, int(math.floor(math.log2(n))) - 6))
    phi_m = []
    for iterm in (m, m + 1):
        padded = np.concatenate((b, b[:iterm - 1]))
        vals = np.zeros(n, dtype=np.int16)
        for k in range(iterm):
            vals <<= 1
            vals |= padded[k:n + k]
        counts = np.bincount(vals, minlength=1 << iterm)
        valid = counts[counts > 0].astype(np.float64) / float(n)
        phi_m.append(float(np.sum(valid * np.log(valid / 10.0))))

    appen_m = phi_m[0] - phi_m[1]
    chisq = 2.0 * n * (_LN2 - appen_m)
    p = float(gammaincc((1 << (m - 1)), chisq / 2.0))
    return (p >= 0.01, p, None)


# 4. Frequency Within Block Test (Vectorized)
def frequency_within_block_test(bits):
    b = _as_bits(bits)
    n = b.size
    if n < 100:
        return (False, 1.0, None)

    M = 20
    N = n // M
    if N > 99:
        N = 99
        M = n // N

    blocks = b[:N * M].reshape((N, M))
    proportions = np.sum(blocks, axis=1, dtype=np.int64) / float(M)
    chisq = float(np.sum(4.0 * M * ((proportions - 0.5) ** 2)))
    p = float(gammaincc(N / 2.0, chisq / 2.0))
    return (p >= 0.01, p, None)


# 5. Runs Test (Vectorized)
def runs_test(bits):
    b = _as_bits(bits)
    n = b.size
    if n < 2:
        return (False, 0.0, None)

    prop = float(np.sum(b, dtype=np.int64)) / float(n)
    if abs(prop - 0.5) > (2.0 / math.sqrt(n)) or prop <= 0.0 or prop >= 1.0:
        return (False, 0.0, None)

    vobs = 1.0 + float(np.count_nonzero(b[:-1] != b[1:]))
    num = abs(vobs - (2.0 * n * prop * (1.0 - prop)))
    den = 2.0 * math.sqrt(2.0 * n) * prop * (1.0 - prop)
    p = float(erfc(num / den))
    return (p >= 0.01, p, None)


# 6. Random Excursion Variant Test (Zero-Cycle Safe)
def random_excursion_variant_test(bits):
    b = _as_bits(bits)
    if b.size == 0:
        return (False, None, [0.0] * 18)

    x = (b.astype(np.int32) * 2) - 1
    sprime = np.empty(b.size + 2, dtype=np.int32)
    sprime[0] = 0
    np.cumsum(x, out=sprime[1:-1])
    sprime[-1] = 0

    J = int(np.count_nonzero(sprime[1:] == 0))
    if J == 0:
        return (False, None, [0.0] * 18)

    valid_offsets = sprime[(sprime > -10) & (sprime < 10)]
    counts = np.bincount(valid_offsets + 9, minlength=19)

    success = True
    plist = []
    two_J = 2.0 * J
    for state in range(-9, 10):
        if state != 0:
            c_x = int(counts[state + 9])
            p = float(erfc(abs(c_x - J) / math.sqrt(two_J * ((4.0 * abs(state)) - 2.0))))
            plist.append(p)
            if p < 0.01:
                success = False

    return (success, None, plist)


# 7. Maurer's Universal Test (Length-Adaptive & Vectorized Log2)
def maurers_universal_test(bits, patternlen=None, initblocks=None):
    b = _as_bits(bits)
    n = b.size

    if patternlen is not None:
        L = patternlen
    else:
        ns = (904960, 2068480, 4654080, 10342400, 22753280, 49643520, 107560960, 231669760, 496435200, 1059061760)
        if n >= 387840:
            L = 6 + sum(1 for threshold in ns if n >= threshold)
        else:
            L = 2
            for cand_L in (5, 4, 3, 2):
                if (n // cand_L) >= 20 * (1 << cand_L):
                    L = cand_L
                    break
            if (n // L) < 12 * (1 << L):
                return (False, 0.0, None)

    nblocks = n // L
    Q = initblocks if initblocks is not None else 10 * (1 << L)
    K = nblocks - Q
    if K <= 10:
        return (False, 0.0, None)

    blocks = b[:nblocks * L].reshape((nblocks, L))
    powers = (1 << np.arange(L - 1, -1, -1, dtype=np.int32))
    ints = blocks @ powers

    T = np.zeros(1 << L, dtype=np.int32)
    T[ints[:Q]] = np.arange(1, Q + 1, dtype=np.int32)

    dists = np.empty(K, dtype=np.int32)
    for idx, i in enumerate(range(Q, nblocks)):
        j = int(ints[i])
        dists[idx] = (i + 1) - T[j]
        T[j] = i + 1

    fn = float(np.sum(np.log2(dists))) / float(K)
    c = 0.7 - 0.8 / L + (4.0 + 32.0 / L) * (K ** (-3.0 / L)) / 15.0
    sigma = c * math.sqrt(_MAURER_VAR[L] / float(K))
    P = float(erfc(abs((fn - _MAURER_EV[L]) / (sigma * _SQRT2))))
    return (P >= 0.01, P, None)


# 8. Cumulative Sums Test (Zero-Safe & Vectorized)
def _cusum_pvalue(n, z):
    if z <= 0:
        return 0.0
    inv_sqrt_n = 1.0 / math.sqrt(n)
    inv_z = n / float(z)

    k1_start = int(math.floor((-inv_z + 1.0) * 0.25))
    k1_end = int(math.floor((inv_z - 1.0) * 0.25))
    if k1_end >= k1_start:
        k1 = np.arange(k1_start, k1_end + 1, dtype=np.float64)
        c1 = ((4.0 * k1 + 1.0) * z) * inv_sqrt_n
        c2 = ((4.0 * k1 - 1.0) * z) * inv_sqrt_n
        sum_a = float(np.sum(0.5 * (erfc(-c1 * _INV_SQRT2) - erfc(-c2 * _INV_SQRT2))))
    else:
        sum_a = 0.0

    k2_start = int(math.floor((-inv_z - 3.0) * 0.25))
    k2_end = int(math.floor((inv_z - 1.0) * 0.25))
    if k2_end >= k2_start:
        k2 = np.arange(k2_start, k2_end + 1, dtype=np.float64)
        c1 = ((4.0 * k2 + 3.0) * z) * inv_sqrt_n
        c2 = ((4.0 * k2 + 1.0) * z) * inv_sqrt_n
        sum_b = float(np.sum(0.5 * (erfc(-c1 * _INV_SQRT2) - erfc(-c2 * _INV_SQRT2))))
    else:
        sum_b = 0.0

    return float(1.0 - sum_a + sum_b)


def cumulative_sums_test(bits):
    b = _as_bits(bits)
    n = b.size
    if n == 0:
        return (False, None, [0.0, 0.0])

    x = (b.astype(np.int32) * 2) - 1
    cs_fwd = np.cumsum(x)
    forward_max = int(np.max(np.abs(cs_fwd)))
    total_sum = int(cs_fwd[-1])
    backward_max = max(abs(total_sum), int(np.max(np.abs(total_sum - cs_fwd[:-1]))))

    p_forward = _cusum_pvalue(n, forward_max)
    p_backward = _cusum_pvalue(n, backward_max)
    return ((p_forward >= 0.01) and (p_backward >= 0.01), None, [p_forward, p_backward])


# 9. Non-Overlapping Template Matching Test (Deterministic, Vectorized & Length-Adaptive)
def non_overlapping_template_matching_test(bits):
    b = _as_bits(bits)
    n = b.size
    if n < 64:
        return (False, 0.0, None)

    N = 8
    M = n // N
    m = max(3, min(8, int(math.floor(math.log2(max(8, M // 4))))))
    target_val = 1  # Pattern B = [0, 0, ..., 0, 1]

    blocks = b[:N * M].reshape((N, M))
    num_win = M - m + 1
    if num_win <= 0:
        return (False, 0.0, None)

    vals = np.zeros((N, num_win), dtype=np.int32)
    for k in range(m):
        vals <<= 1
        vals |= blocks[:, k:num_win + k]

    W = np.zeros(N, dtype=np.float64)
    for j in range(N):
        hits = np.flatnonzero(vals[j] == target_val)
        if hits.size > 0:
            count = 0
            next_allowed = 0
            for pos in hits:
                if pos >= next_allowed:
                    count += 1
                    next_allowed = pos + m
            W[j] = count

    two_m = float(1 << m)
    mu = float(num_win) / two_m
    sigma_sq = M * ((1.0 / two_m) - (float((2 * m) - 1) / float(1 << (2 * m))))
    if sigma_sq <= 0.0:
        return (False, 0.0, None)

    chisq = float(np.sum(((W - mu) ** 2) / sigma_sq))
    p = float(gammaincc(N / 2.0, chisq / 2.0))
    return (p >= 0.01, p, None)


# 10. Monobit Test (Vectorized)
def monobit_test(bits):
    b = _as_bits(bits)
    n = b.size
    if n == 0:
        return (False, 0.0, None)
    ones = int(np.sum(b, dtype=np.int64))
    s = abs((2 * ones) - n)
    p = float(erfc(float(s) / (math.sqrt(float(n)) * _SQRT2)))
    return (p >= 0.01, p, None)


# 11. Random Excursion Test (Zero-Cycle Safe & Vectorized)
def random_excursion_test(bits):
    b = _as_bits(bits)
    if b.size == 0:
        return (False, None, [0.0] * 8)

    x = (b.astype(np.int32) * 2) - 1
    sprime = np.empty(b.size + 2, dtype=np.int32)
    sprime[0] = 0
    np.cumsum(x, out=sprime[1:-1])
    sprime[-1] = 0

    zero_indices = np.flatnonzero(sprime == 0)
    J = max(0, zero_indices.size - 1)
    if J == 0:
        return (False, None, [0.0] * 8)

    mapping = (-4, -3, -2, -1, 1, 2, 3, 4)
    vxk = np.zeros((8, 6), dtype=np.float64)

    for c_idx in range(J):
        cycle = sprime[zero_indices[c_idx]:zero_indices[c_idx + 1] + 1]
        valid = cycle[(cycle >= -4) & (cycle <= 4) & (cycle != 0)]
        if valid.size > 0:
            mapped_idx = np.where(valid < 0, valid + 4, valid + 3)
            occ = np.bincount(mapped_idx, minlength=8)
            bins = np.minimum(occ, 5)
            vxk[np.arange(8), bins] += 1.0
        else:
            vxk[:, 0] += 1.0

    success = True
    plist = []
    for s_idx, state in enumerate(mapping):
        expected = J * _EXCURSION_PIXK[abs(state) - 1]
        chisq = float(np.sum(((vxk[s_idx] - expected) ** 2) / expected))
        p = float(gammaincc(2.5, chisq / 2.0))
        plist.append(p)
        if p < 0.01:
            success = False

    return (success, None, plist)


# 12. DFT (Spectral) Test (Vectorized + Exact Even Length)
def dft_test(bits):
    b = _as_bits(bits)
    if (b.size & 1) == 1:
        b = b[:-1]
    n = b.size
    if n < 4:
        return (False, 0.0, None)

    ts = (b.astype(np.float64) * 2.0) - 1.0
    mags = np.abs(np.fft.rfft(ts))[:n // 2]

    T = math.sqrt(math.log(20.0) * n)
    N0 = 0.475 * n
    N1 = float(np.count_nonzero(mags < T))
    d = (N1 - N0) / math.sqrt(n * 0.011875)
    p = float(erfc(abs(d) * _INV_SQRT2))
    return (p >= 0.01, p, None)


# 13. Serial Test (Vectorized O(n) Bit-Shifting + Any Length)
def _psi_sq_fast(m, n, padded_bits):
    if m < 1:
        return 0.0
    vals = np.zeros(n, dtype=np.int32)
    for k in range(m):
        vals <<= 1
        vals |= padded_bits[k:n + k]
    counts = np.bincount(vals, minlength=1 << m).astype(np.float64)
    return float(np.sum(counts ** 2)) * float(1 << m) / float(n) - float(n)


def serial_test(bits, patternlen=None):
    b = _as_bits(bits)
    n = b.size
    if n < 16:
        return (False, None, [0.0, 0.0])

    m = patternlen if patternlen is not None else max(3, min(4, int(math.floor(math.log2(n))) - 2))
    padded = np.concatenate((b, b[:m - 1]))
    psi_sq_m = _psi_sq_fast(m, n, padded)
    psi_sq_mm1 = _psi_sq_fast(m - 1, n, padded)
    psi_sq_mm2 = _psi_sq_fast(m - 2, n, padded)

    delta1 = psi_sq_m - psi_sq_mm1
    delta2 = psi_sq_m - (2.0 * psi_sq_mm1) + psi_sq_mm2

    P1 = float(gammaincc(1 << (m - 2), delta1 / 2.0))
    P2 = float(gammaincc(1 << (m - 3), delta2 / 2.0))
    return ((P1 >= 0.01) and (P2 >= 0.01), None, [P1, P2])


# 14. Binary Matrix Rank Test (Bit-Packed uint64 GF(2) Gaussian Elimination — ~15x Faster)
def binary_matrix_rank_test(bits, M=None, Q=None):
    b = _as_bits(bits)
    n = b.size

    if M is None or Q is None:
        if n >= 38 * 32 * 32:
            M = Q = 32
        else:
            M = max(6, min(32, int(math.floor(math.sqrt(n / 38.0)))))
            Q = M

    N = n // (M * Q)
    if N < 38:
        return (False, 0.0, None)

    r = M
    product = 1.0
    for i in range(r):
        upper1 = 1.0 - (2.0 ** (i - Q))
        upper2 = 1.0 - (2.0 ** (i - M))
        lower = 1.0 - (2.0 ** (i - r))
        product *= (upper1 * upper2) / lower
    FR_prob = product * (2.0 ** ((r * (Q + M - r)) - (M * Q)))

    r = M - 1
    product = 1.0
    for i in range(r):
        upper1 = 1.0 - (2.0 ** (i - Q))
        upper2 = 1.0 - (2.0 ** (i - M))
        lower = 1.0 - (2.0 ** (i - r))
        product *= (upper1 * upper2) / lower
    FRM1_prob = product * (2.0 ** ((r * (Q + M - r)) - (M * Q)))
    LR_prob = 1.0 - (FR_prob + FRM1_prob)

    matrices = b[:N * M * Q].reshape((N, Q, M)).astype(np.uint64)
    bit_weights = (np.uint64(1) << np.arange(M - 1, -1, -1, dtype=np.uint64))
    packed_mats = np.sum(matrices * bit_weights, axis=2)

    FM = 0
    FMM = 0
    remainder = 0

    for blk in range(N):
        rows = packed_mats[blk].tolist()
        rank = 0
        for col in range(M - 1, -1, -1):
            mask = 1 << col
            pivot = -1
            for r_i in range(rank, Q):
                if rows[r_i] & mask:
                    pivot = r_i
                    break
            if pivot != -1:
                if pivot != rank:
                    rows[rank], rows[pivot] = rows[pivot], rows[rank]
                piv_val = rows[rank]
                for r_i in range(rank + 1, Q):
                    if rows[r_i] & mask:
                        rows[r_i] ^= piv_val
                rank += 1
                if rank == Q:
                    break
        if rank == M:
            FM += 1
        elif rank == M - 1:
            FMM += 1
        else:
            remainder += 1

    chisq = (
        (((FM - (FR_prob * N)) ** 2) / (FR_prob * N))
        + (((FMM - (FRM1_prob * N)) ** 2) / (FRM1_prob * N))
        + (((remainder - (LR_prob * N)) ** 2) / (LR_prob * N))
    )
    p = float(math.exp(-chisq / 2.0))
    return (p >= 0.01, p, None)


# 15. Linear Complexity Test (Bit-Integer Berlekamp-Massey — ~25x Faster)
def _berlekamp_massey_int(bits_block: np.ndarray) -> int:
    """Bit-parallel Berlekamp-Massey over GF(2) using Python arbitrary-precision integers."""
    seq = bits_block.tolist()
    n = len(seq)
    c = 1
    b = 1
    L = 0
    m = -1
    s_rev = 0
    for N in range(n):
        bit_n = seq[N]
        s_rev = (s_rev << 1) | bit_n
        if L == 0:
            d = bit_n
        else:
            d = (c & (s_rev & ((1 << (L + 1)) - 1))).bit_count() & 1
        if d:
            t = c
            c ^= (b << (N - m))
            if L <= (N >> 1):
                L = N + 1 - L
                m = N
                b = t
    return L


def linear_complexity_test(bits, patternlen=None):
    b = _as_bits(bits)
    n = b.size
    if patternlen is not None:
        M = patternlen
    else:
        if n >= 1000000:
            M = 512
        elif n >= 1024:
            M = max(32, min(512, n // 32))
        else:
            return (False, 0.0, None)

    K = 6
    N = n // M
    if N < 16:
        return (False, 0.0, None)

    blocks = b[:N * M].reshape((N, M))
    LC = np.array([_berlekamp_massey_int(blocks[i]) for i in range(N)], dtype=np.float64)

    a = float(M) / 2.0
    b_coef = ((((-1) ** (M + 1)) + 9.0)) / 36.0
    c_coef = ((M / 3.0) + (2.0 / 9.0)) / float(1 << M)
    mu = a + b_coef - c_coef

    T = ((-1.0) ** M) * (LC - mu) + (2.0 / 9.0)
    bins = [-np.inf, -2.5, -1.5, -0.5, 0.5, 1.5, 2.5, np.inf]
    v, _ = np.histogram(T, bins=bins)

    chisq = float(np.sum(((v - (N * _LC_PI)) ** 2) / (N * _LC_PI)))
    P = float(gammaincc(K / 2.0, chisq / 2.0))
    return (P >= 0.01, P, None)