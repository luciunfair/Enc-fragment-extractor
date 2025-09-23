import numpy as np
from scipy.stats import chi2, chisquare
import pandas as pd

def chi2_uniform(row, Length=4096):
    """
    Computes the chi-square statistic for a given array of bytes
    to test if it follows a uniform distribution.
    
    Parameters:
        row (numpy.ndarray): A numpy array of byte values (0-255).
        Length (int): Total number of bytes in the fragment.
    
    Returns:
        float: The chi-square statistic.
    """
    # Expected frequency for uniform distribution
    Ei = Length / 256
    
    # Count occurrences of each byte value (0-255)
    observed_counts = np.bincount(row, minlength=256)
    
    # Compute chi-square statistic using vectorized operations
    chi2_stat = np.sum(((observed_counts - Ei) ** 2) / Ei)
    
    # Degrees of freedom (df = 256 - 1)
    df = 255

    # Compute p-value using chi-square survival function (1 - CDF)
    p_value = 1 - chi2.cdf(chi2_stat, df)

        
    return chi2_stat, p_value

def chi_test_absolute(chi2_uniform_stat, gamma=2, length=4096):
    if length==1024:
        success = (1 if (255.02-gamma*22.57)<=chi2_uniform_stat<=(255.02+gamma*22.57) else 0)
    elif length==2048:
        success = (1 if (254.98-gamma*22.57)<=chi2_uniform_stat<=(254.98+gamma*22.57) else 0)
    elif length==4096:
        success = (1 if (255.04-gamma*22.60)<=chi2_uniform_stat<=(255.04+gamma*22.60) else 0)
    elif length==8192:
        success = (1 if (255.09-gamma*22.54)<=chi2_uniform_stat<=(255.09+gamma*22.54) else 0)
    elif length==16.384:
        success = (1 if (254.96-gamma*22.76)<=chi2_uniform_stat<=(254.96+gamma*22.76) else 0)  
    elif length==32.768:
        success = (1 if (255.08-gamma*22.68)<=chi2_uniform_stat<=(255.08+gamma*22.68) else 0)     
    elif length==65.536:
        success = (1 if (255.37-gamma*22.82)<=chi2_uniform_stat<=(255.37+gamma*22.82) else 0)            
    return success

def chi_test_confidence(chi2_uniform_pvalue):
    
    success = (0 if chi2_uniform_pvalue<0.01 or chi2_uniform_pvalue>0.99 else 1)
    return success