
import pandas as pd

# Input CSV files
input_files = [
    "E:\\dataset\\itc\\unenc_val.csv",
    "E:\\dataset\\encod\\unenc_val.csv",
    "E:\\dataset\\itc\\enc_val.csv",
    "E:\\dataset\\encod\\enc_val.csv"
]

samples_per_file = 5
output_file = "random_samples.csv"

all_samples = []

for file in input_files:
    # Read CSV without header
    df = pd.read_csv(file, header=None)

    if len(df) < samples_per_file:
        raise ValueError(f"{file} has only {len(df)} rows!")

    # Randomly select 5 rows
    samples = df.sample(n=samples_per_file)

    all_samples.append(samples)

# Concatenate in input file order
result = pd.concat(all_samples, ignore_index=True)

# Save without header and index
result.to_csv(output_file, index=False, header=False)

print(f"Done! {len(result)} samples saved to {output_file}")