import numpy as np
def byte_frequency_distribution(byte_sequence):
    """Calculates Byte Frequency Distribution (256 values)."""
    bfd = np.zeros(256, dtype=np.float32)
    if len(byte_sequence) == 0:
        return bfd
    for byte_val in byte_sequence:
        bfd[int(byte_val)] += 1
    return bfd / len(byte_sequence) # Normalize

def discrete_fourier_transform(byte_sequence, num_coeffs=5):
    """Calculates DFT features: 'num_coeffs' highest magnitude coefficients and their indices (2 * num_coeffs values)."""
    sequence_len = len(byte_sequence)
    if sequence_len < 2:
        return np.zeros(2 * num_coeffs, dtype=np.float32)
    fft_coeffs = np.fft.fft(byte_sequence)
    unique_magnitudes_len = sequence_len // 2 + 1
    magnitudes = np.abs(fft_coeffs[:unique_magnitudes_len])
    if len(magnitudes) == 0:
        return np.zeros(2 * num_coeffs, dtype=np.float32)
    actual_num_coeffs_to_select = min(num_coeffs, len(magnitudes))
    if actual_num_coeffs_to_select == 0:
        return np.zeros(2 * num_coeffs, dtype=np.float32)
    top_indices_sorted_by_magnitude = np.argsort(magnitudes)[-actual_num_coeffs_to_select:]
    top_magnitudes_selected = magnitudes[top_indices_sorted_by_magnitude]
    dft_feats = np.zeros(2 * num_coeffs, dtype=np.float32)
    sum_all_unique_magnitudes = np.sum(magnitudes)
    top_magnitudes_normalized = top_magnitudes_selected / sum_all_unique_magnitudes if sum_all_unique_magnitudes > 0 else top_magnitudes_selected
    for i in range(actual_num_coeffs_to_select):
        dft_feats[i] = top_magnitudes_normalized[i]
        if (sequence_len // 2) > 0 :
            dft_feats[i + num_coeffs] = float(top_indices_sorted_by_magnitude[i]) / (sequence_len // 2)
        else:
            dft_feats[i + num_coeffs] = 0.0
    return dft_feats


def lempel_ziv_complexity(byte_sequence_np):
    """Calculates Lempel-Ziv complexity (LZ76 variant simplified)."""
    if byte_sequence_np.size == 0: return 0.0
    data = [str(b) for b in byte_sequence_np] # Convert bytes to strings for dictionary keys
    n = len(data)
    if n == 0: return 0.0
    dictionary = {}
    w = ""
    complexity = 0
    for i in range(n):
        c = data[i]
        wc = w + c
        if wc in dictionary:
            w = wc
        else:
            dictionary[wc] = len(dictionary) # Store the new phrase
            w = c # Start new phrase with current char
            complexity += 1
            # Crucial for LZ76: if the single character 'c' itself starts a new phrase
            # and is not yet in the dictionary (only for the very first occurrence of 'c' as a new phrase start)
            if w not in dictionary:
                 dictionary[w] = len(dictionary)
    return float(complexity)

def monte_carlo_pi_approximation(byte_sequence):
    """Calculates Monte Carlo pi approximation feature (1 value)."""
    n = len(byte_sequence)
    if n < 2: return 0.0
    num_pairs = n // 2
    if num_pairs == 0: return 0.0
    points_in_circle = 0
    for i in range(num_pairs):
        x = byte_sequence[2*i] / 255.0; y = byte_sequence[2*i + 1] / 255.0
        if (x - 0.5)**2 + (y - 0.5)**2 <= 0.5**2: points_in_circle += 1
    pi_approximation = 4.0 * (points_in_circle / num_pairs) if num_pairs > 0 else 0.0
    return float(pi_approximation)