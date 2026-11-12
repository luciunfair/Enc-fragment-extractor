import math
import numpy as np
from scipy.special import gammaincc, erfc


def _as_bits(bits):
    return np.asarray(bits, dtype=np.uint8)


def longest_run_ones_in_a_block_test(bits):
    b = _as_bits(bits)
    n = b.size
    if n < 128:
        return (False, 1.0, None)
    elif n < 6272:
        M, K, N = 8, 3, 16
        probs = [0.2148, 0.3672, 0.2305, 0.1875]
    elif n < 750000:
        M, K, N = 128, 5, 49
        probs = [0.1174, 0.2430, 0.2493, 0.1752, 0.1027, 0.1124]
    else:
        M, K, N = 10000, 6, 75
        probs = [0.0882, 0.2092, 0.2483, 0.1933, 0.1208, 0.0675, 0.0727]

    blocks = b[:N * M].reshape((N, M))
    v = np.zeros(K + 1, dtype=np.int64)

    for i in range(N):
        blk = blocks[i]
        padded = np.concatenate(([0], blk, [0]))
        diffs = np.diff(padded)
        starts = np.flatnonzero(diffs == 1)
        ends = np.flatnonzero(diffs == -1)
        longest = int(np.max(ends - starts)) if starts.size > 0 else 0

        if M == 8:
            idx = 0 if longest <= 1 else (3 if longest >= 4 else longest - 1)
        elif M == 128:
            idx = 0 if longest <= 4 else (5 if longest >= 9 else longest - 4)
        else:
            idx = 0 if longest <= 10 else (6 if longest >= 16 else longest - 10)
        v[idx] += 1

    pi = np.array(probs, dtype=np.float64)
    chi_sq = float(np.sum(((v - N * pi) ** 2) / (N * pi)))
    p = float(gammaincc(K / 2.0, chi_sq / 2.0))
    return (p >= 0.01, p, None)


def overlapping_template_matching_test(bits, blen=6):
    b = _as_bits(bits)
    n = b.size
    if n < 512:
        return (False, 0.0, None)

    if n >= 1028016:
        m, M, N = 10, 1062, n // 1062
    else:
        m = max(4, min(9, int(math.floor(math.log2(n / 32.0))) - 1))
        M = max(32, 2 ** (m + 1))
        N = n // M
        if N < 16:
            N = 16
            M = n // N
        if M <= m + 1:
            return (False, 0.0, None)

    K = 5
    blocks = b[:N * M].reshape((N, M))
    cs = np.cumsum(np.pad(blocks, ((0, 0), (1, 0))), axis=1)
    win_sums = cs[:, m:] - cs[:, :-m]
    counts = np.sum(win_sums == m, axis=1)
    v = np.bincount(np.minimum(counts, K), minlength=K + 1)

    lambd = (M - m + 1.0) / (2.0 ** m)
    eta = lambd / 2.0

    pi = np.zeros(K + 1, dtype=np.float64)
    pi[0] = math.exp(-eta)
    for u in range(1, K):
        s = 0.0
        for l in range(1, u + 1):
            log_term = (
                -eta - u * math.log(2.0) + l * math.log(eta)
                - math.lgamma(l + 1) + math.lgamma(u) - math.lgamma(l) - math.lgamma(u - l + 1)
            )
            s += math.exp(log_term)
        pi[u] = s
    pi[K] = max(1e-12, 1.0 - float(np.sum(pi[:K])))

    expected = N * pi
    chisq = float(np.sum(((v - expected) ** 2) / np.maximum(expected, 1e-12)))
    p = float(gammaincc(K / 2.0, chisq / 2.0))
    return (p >= 0.01, p, None)


def approximate_entropy_test(bits):
    b = _as_bits(bits)
    n = b.size
    if n < 16:
        return (False, 0.0, None)

    m = max(2, min(3, int(math.floor(math.log2(n))) - 6))
    phi_m = []
    for iterm in range(m, m + 2):
        padded = np.concatenate((b, b[:iterm - 1]))
        vals = np.zeros(n, dtype=np.int16)
        for k in range(iterm):
            vals <<= 1
            vals |= padded[k:n + k]
        counts = np.bincount(vals, minlength=2 ** iterm)
        Ci = counts.astype(np.float64) / float(n)
        valid = Ci[Ci > 0]
        phi_m.append(float(np.sum(valid * np.log(valid / 10.0))))

    appen_m = phi_m[0] - phi_m[1]
    chisq = 2.0 * n * (math.log(2.0) - appen_m)
    p = float(gammaincc(2 ** (m - 1), chisq / 2.0))
    return (p >= 0.01, p, None)


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
    ones = np.sum(blocks, axis=1)
    proportions = ones / float(M)
    chisq = float(np.sum(4.0 * M * ((proportions - 0.5) ** 2)))
    p = float(gammaincc(N / 2.0, chisq / 2.0))
    return (p >= 0.01, p, None)


def runs_test(bits):
    b = _as_bits(bits)
    n = b.size
    if n < 2:
        return (False, 0.0, None)

    ones = int(np.sum(b))
    prop = ones / float(n)
    tau = 2.0 / math.sqrt(n)
    if abs(prop - 0.5) > tau or prop <= 0.0 or prop >= 1.0:
        return (False, 0.0, None)

    vobs = 1.0 + float(np.sum(b[:-1] != b[1:]))
    num = abs(vobs - (2.0 * n * prop * (1.0 - prop)))
    den = 2.0 * math.sqrt(2.0 * n) * prop * (1.0 - prop)
    p = float(erfc(num / den))
    return (p >= 0.01, p, None)


def random_excursion_variant_test(bits):
    b = _as_bits(bits)
    if b.size == 0:
        return (False, None, [0.0] * 18)

    x = (b.astype(np.int32) * 2) - 1
    s = np.cumsum(x)
    sprime = np.concatenate(([0], s, [0]))

    J = int(np.sum(sprime[1:] == 0))
    if J == 0:
        return (False, None, [0.0] * 18)

    valid_offsets = sprime[np.abs(sprime) < 10]
    counts = np.bincount(valid_offsets + 9, minlength=19)

    success = True
    plist = []
    for state in range(-9, 10):
        if state != 0:
            c_x = int(counts[state + 9])
            top = abs(c_x - J)
            bottom = math.sqrt(2.0 * J * ((4.0 * abs(state)) - 2.0))
            p = float(erfc(top / bottom))
            plist.append(p)
            if p < 0.01:
                success = False

    return (success, None, plist)


def maurers_universal_test(bits, patternlen=None, initblocks=None):
    b = _as_bits(bits)
    n = b.size

    ev_table = [0, 0.73264948, 1.5374383, 2.40160681, 3.31122472,
                4.25342659, 5.2177052, 6.1962507, 7.1836656,
                8.1764248, 9.1723243, 10.170032, 11.168765,
                12.168070, 13.167693, 14.167488, 15.167379]
    var_table = [0, 0.690, 1.338, 1.901, 2.358, 2.705, 2.954, 3.125,
                 3.238, 3.311, 3.356, 3.384, 3.401, 3.410, 3.416,
                 3.419, 3.421]

    if patternlen is not None:
        L = patternlen
    else:
        ns = [904960, 2068480, 4654080, 10342400,
              22753280, 49643520, 107560960,
              231669760, 496435200, 1059061760]
        if n >= 387840:
            L = 6
            for threshold in ns:
                if n >= threshold:
                    L += 1
        else:
            L = 2
            for cand_L in (5, 4, 3, 2):
                if (n // cand_L) >= 20 * (2 ** cand_L):
                    L = cand_L
                    break
            if (n // L) < 12 * (2 ** L):
                return (False, 0.0, None)

    nblocks = n // L
    Q = initblocks if initblocks is not None else 10 * (2 ** L)
    K = nblocks - Q
    if K <= 10:
        return (False, 0.0, None)

    blocks = b[:nblocks * L].reshape((nblocks, L))
    powers = (1 << np.arange(L - 1, -1, -1, dtype=np.int32))
    ints = blocks @ powers

    T = np.zeros(2 ** L, dtype=np.int32)
    for i in range(Q):
        T[ints[i]] = i + 1

    total_log = 0.0
    for i in range(Q, nblocks):
        j = ints[i]
        dist = (i + 1) - T[j]
        T[j] = i + 1
        total_log += math.log2(dist)

    fn = total_log / float(K)
    c = 0.7 - 0.8 / L + (4.0 + 32.0 / L) * (K ** (-3.0 / L)) / 15.0
    sigma = c * math.sqrt(var_table[L] / float(K))
    mag = abs((fn - ev_table[L]) / (sigma * math.sqrt(2.0)))
    P = float(erfc(mag))
    return (P >= 0.01, P, None)


def _cusum_pvalue(n, z):
    if z <= 0:
        return 0.0
    sqrt_n = math.sqrt(n)
    sum_a = 0.0
    startk = int(math.floor(((-n / float(z)) + 1.0) / 4.0))
    endk = int(math.floor(((n / float(z)) - 1.0) / 4.0))
    for k in range(startk, endk + 1):
        c1 = (((4.0 * k) + 1.0) * z) / sqrt_n
        c2 = (((4.0 * k) - 1.0) * z) / sqrt_n
        sum_a += (0.5 * erfc(-c1 * math.sqrt(0.5))) - (0.5 * erfc(-c2 * math.sqrt(0.5)))

    sum_b = 0.0
    startk = int(math.floor(((-n / float(z)) - 3.0) / 4.0))
    endk = int(math.floor(((n / float(z)) - 1.0) / 4.0))
    for k in range(startk, endk + 1):
        c1 = (((4.0 * k) + 3.0) * z) / sqrt_n
        c2 = (((4.0 * k) + 1.0) * z) / sqrt_n
        sum_b += (0.5 * erfc(-c1 * math.sqrt(0.5))) - (0.5 * erfc(-c2 * math.sqrt(0.5)))

    return float(1.0 - sum_a + sum_b)


def cumulative_sums_test(bits):
    b = _as_bits(bits)
    n = b.size
    if n == 0:
        return (False, None, [0.0, 0.0])

    x = (b.astype(np.int32) * 2) - 1
    forward_max = int(np.max(np.abs(np.cumsum(x))))
    backward_max = int(np.max(np.abs(np.cumsum(x[::-1]))))

    p_forward = _cusum_pvalue(n, forward_max)
    p_backward = _cusum_pvalue(n, backward_max)
    success = (p_forward >= 0.01) and (p_backward >= 0.01)
    return (success, None, [p_forward, p_backward])


def non_overlapping_template_matching_test(bits):
    b = _as_bits(bits)
    n = b.size
    if n < 64:
        return (False, 0.0, None)

    N = 8
    M = n // N
    m = max(3, min(8, int(math.floor(math.log2(max(8, M // 4))))))
    B = [0] * (m - 1) + [1]

    blocks = b[:N * M].reshape((N, M))
    W = np.zeros(N, dtype=np.float64)
    for j in range(N):
        blk = blocks[j].tolist()
        pos = 0
        count = 0
        while pos <= (M - m):
            if blk[pos:pos + m] == B:
                pos += m
                count += 1
            else:
                pos += 1
        W[j] = count

    mu = float(M - m + 1) / float(2 ** m)
    sigma_sq = M * ((1.0 / float(2 ** m)) - (float((2 * m) - 1) / float(2 ** (2 * m))))
    if sigma_sq <= 0.0:
        return (False, 0.0, None)

    chisq = float(np.sum(((W - mu) ** 2) / sigma_sq))
    p = float(gammaincc(N / 2.0, chisq / 2.0))
    return (p >= 0.01, p, None)


def monobit_test(bits):
    b = _as_bits(bits)
    n = b.size
    if n == 0:
        return (False, 0.0, None)
    ones = int(np.sum(b))
    zeroes = n - ones
    s = abs(ones - zeroes)
    p = float(erfc(float(s) / (math.sqrt(float(n)) * math.sqrt(2.0))))
    return (p >= 0.01, p, None)


def random_excursion_test(bits):
    b = _as_bits(bits)
    if b.size == 0:
        return (False, None, [0.0] * 8)

    x = (b.astype(np.int32) * 2) - 1
    s = np.cumsum(x)
    sprime = np.concatenate(([0], s, [0]))

    zero_indices = np.flatnonzero(sprime == 0)
    J = max(0, zero_indices.size - 1)
    if J == 0:
        return (False, None, [0.0] * 8)

    mapping = [-4, -3, -2, -1, 1, 2, 3, 4]
    vxk = np.zeros((8, 6), dtype=np.float64)

    for c_idx in range(J):
        cycle = sprime[zero_indices[c_idx]:zero_indices[c_idx + 1] + 1]
        for s_idx, state in enumerate(mapping):
            oc = int(np.sum(cycle == state))
            k_bin = 5 if oc >= 5 else oc
            vxk[s_idx, k_bin] += 1.0

    pixk = np.array([
        [0.5,    0.25,   0.125,  0.0625, 0.0312, 0.0312],
        [0.75,   0.0625, 0.0469, 0.0352, 0.0264, 0.0791],
        [0.8333, 0.0278, 0.0231, 0.0193, 0.0161, 0.0804],
        [0.875,  0.0156, 0.0137, 0.012,  0.0105, 0.0733],
        [0.9,    0.01,   0.009,  0.0081, 0.0073, 0.0656],
        [0.9167, 0.0069, 0.0064, 0.0058, 0.0053, 0.0588],
        [0.9286, 0.0051, 0.0047, 0.0044, 0.0041, 0.0531]
    ], dtype=np.float64)

    success = True
    plist = []
    for s_idx, state in enumerate(mapping):
        probs = pixk[abs(state) - 1]
        expected = J * probs
        chisq = float(np.sum(((vxk[s_idx] - expected) ** 2) / expected))
        p = float(gammaincc(2.5, chisq / 2.0))
        plist.append(p)
        if p < 0.01:
            success = False

    return (success, None, plist)


def dft_test(bits):
    b = _as_bits(bits)
    if (b.size % 2) == 1:
        b = b[:-1]
    n = b.size
    if n < 4:
        return (False, 0.0, None)

    ts = (b.astype(np.float64) * 2.0) - 1.0
    fs = np.fft.rfft(ts)
    mags = np.abs(fs)[:n // 2]

    T = math.sqrt(math.log(1.0 / 0.05) * n)
    N0 = 0.95 * n / 2.0
    N1 = float(np.sum(mags < T))
    d = (N1 - N0) / math.sqrt((n * 0.95 * 0.05) / 4.0)
    p = float(erfc(abs(d) / math.sqrt(2.0)))
    return (p >= 0.01, p, None)


def _psi_sq_fast(m, n, padded_bits):
    if m < 1:
        return 0.0
    vals = np.zeros(n, dtype=np.int32)
    for k in range(m):
        vals <<= 1
        vals |= padded_bits[k:n + k]
    counts = np.bincount(vals, minlength=2 ** m).astype(np.float64)
    psi_sq = float(np.sum(counts ** 2)) * (2 ** m) / float(n) - float(n)
    return psi_sq


def serial_test(bits, patternlen=None):
    b = _as_bits(bits)
    n = b.size
    if n < 16:
        return (False, None, [0.0, 0.0])

    if patternlen is not None:
        m = patternlen
    else:
        m = max(3, min(4, int(math.floor(math.log2(n))) - 2))

    padded = np.concatenate((b, b[:m - 1]))
    psi_sq_m = _psi_sq_fast(m, n, padded)
    psi_sq_mm1 = _psi_sq_fast(m - 1, n, padded)
    psi_sq_mm2 = _psi_sq_fast(m - 2, n, padded)

    delta1 = psi_sq_m - psi_sq_mm1
    delta2 = psi_sq_m - (2.0 * psi_sq_mm1) + psi_sq_mm2

    P1 = float(gammaincc(2 ** (m - 2), delta1 / 2.0))
    P2 = float(gammaincc(2 ** (m - 3), delta2 / 2.0))
    success = (P1 >= 0.01) and (P2 >= 0.01)
    return (success, None, [P1, P2])


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

    matrices = b[:N * M * Q].reshape((N, Q, M)).copy()
    FM = 0
    FMM = 0
    remainder = 0

    for blk in range(N):
        mat = matrices[blk]
        row = 0
        for col in range(M):
            if row >= Q:
                break
            pivots = np.flatnonzero(mat[row:, col])
            if pivots.size > 0:
                p_row = row + int(pivots[0])
                if p_row != row:
                    mat[[row, p_row]] = mat[[p_row, row]]
                elim = np.flatnonzero(mat[row + 1:, col]) + (row + 1)
                if elim.size > 0:
                    mat[elim] ^= mat[row]
                row += 1
        if row == M:
            FM += 1
        elif row == M - 1:
            FMM += 1
        else:
            remainder += 1

    chisq = (((FM - (FR_prob * N)) ** 2) / (FR_prob * N))
    chisq += (((FMM - (FRM1_prob * N)) ** 2) / (FRM1_prob * N))
    chisq += (((remainder - (LR_prob * N)) ** 2) / (LR_prob * N))
    p = float(math.exp(-chisq / 2.0))
    return (p >= 0.01, p, None)


def _berlekamp_massey_fast(bits_block):
    n = len(bits_block)
    b = np.zeros(n, dtype=np.uint8)
    c = np.zeros(n, dtype=np.uint8)
    b[0] = 1
    c[0] = 1
    L = 0
    m = -1
    for N in range(n):
        if L == 0:
            d = int(bits_block[N])
        else:
            d = int(bits_block[N]) ^ int(np.bitwise_xor.reduce(c[1:L + 1] & bits_block[N - L:N][::-1]))
        if d != 0:
            t = c.copy()
            span = n - N + m
            c[N - m:N - m + span] ^= b[:span]
            if L <= (N // 2):
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
    LC = np.array([_berlekamp_massey_fast(blocks[i]) for i in range(N)], dtype=np.float64)

    a = float(M) / 2.0
    b_coef = ((((-1) ** (M + 1)) + 9.0)) / 36.0
    c_coef = ((M / 3.0) + (2.0 / 9.0)) / (2.0 ** M)
    mu = a + b_coef - c_coef

    T = ((-1.0) ** M) * (LC - mu) + (2.0 / 9.0)
    bins = [-np.inf, -2.5, -1.5, -0.5, 0.5, 1.5, 2.5, np.inf]
    v, _ = np.histogram(T, bins=bins)

    pi = np.array([0.010417, 0.03125, 0.125, 0.5, 0.25, 0.0625, 0.020833], dtype=np.float64)
    chisq = float(np.sum(((v - (N * pi)) ** 2) / (N * pi)))
    P = float(gammaincc(K / 2.0, chisq / 2.0))
    return (P >= 0.01, P, None)