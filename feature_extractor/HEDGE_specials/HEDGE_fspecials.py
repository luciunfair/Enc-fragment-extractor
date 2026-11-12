import math
import numpy as np
from scipy.stats import chi2


def chi2_uniform(row, Length=None):
    """
    Computes the Pearson chi-square statistic and p-value for a byte array
    of ANY length against a discrete uniform distribution over {0..255}.
    """
    arr = np.asarray(row, dtype=np.uint8)
    L = arr.size if Length is None else int(Length)
    if L <= 0:
        return 0.0, 0.0

    Ei = L / 256.0
    observed_counts = np.bincount(arr, minlength=256).astype(np.float64)
    chi2_stat = float(np.sum(((observed_counts - Ei) ** 2) / Ei))
    p_value = float(chi2.sf(chi2_stat, df=255))

    return chi2_stat, p_value


def chi_test_absolute(chi2_uniform_stat, gamma=2, length=4096):
    """
    HEDGE Absolute Chi-Square Band Test for ANY fragment length L.
    Uses empirical HEDGE reference parameters for standard power-of-two lengths,
    and exact finite-sample multinomial moments (mu=255, sigma=sqrt(510 + 64770/L))
    for any arbitrary fragment length L.
    """
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

    lower = mu - gamma * sigma
    upper = mu + gamma * sigma
    return 1 if (lower <= chi2_uniform_stat <= upper) else 0


def chi_test_confidence(chi2_uniform_pvalue):
    """
    HEDGE Confidence Interval Test (0.01 <= p-value <= 0.99).
    Length-invariant because p-value follows U(0, 1) under H0 for any L.
    """
    return 0 if (chi2_uniform_pvalue < 0.01 or chi2_uniform_pvalue > 0.99) else 1