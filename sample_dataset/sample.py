
import pandas as pd

# Input CSV files
input_files = [
    "E:\\dataset\\itc\\unenc_val.csv",
    "E:\\dataset\\encod\\unenc_val.csv",
    "E:\\dataset\\itc\\enc_val.csv",
    "E:\\dataset\\encod\\enc_val.csv"
]

# Number of random samples from each file
samples_per_file = 5

# Output file
output_file = "sample_dataset.csv"

# Fixed random seed (optional)
random_seed = 42

# Collect samples
all_samples = []

for file in input_files:
    df = pd.read_csv(file)

    if len(df) < samples_per_file:
        raise ValueError(
            f"{file} has only {len(df)} rows!"
        )

    # Randomly select 5 rows
    samples = df.sample(
        n=samples_per_file,
        random_state=random_seed
    )

    all_samples.append(samples)

# Merge all samples in order
result = pd.concat(all_samples, ignore_index=True)

# Save output
result.to_csv(output_file, index=False)

print(f"Done! {len(result)} samples saved to {output_file}")
