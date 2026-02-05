import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import pairwise_distances_argmin_min

def choose_k_centers(csv_input_path_1: str, csv_input_path_2: str, csv_output_path: str, n_centers: int = 4):
    """
    Load two CSV files with feature data, concatenate, shuffle,
    use first 80% rows to train KMeans with n centers,
    then save cluster centers to CSV.
    
    Args:
        csv_input_path_1: Path to first input CSV file
        csv_input_path_2: Path to second input CSV file
        csv_output_path: Path to save cluster centers CSV
        n_centers: Number of clusters
    """
    # Load data assuming no header - change if your CSVs have headers
    df1 = pd.read_csv(csv_input_path_1, header=None)
    df2 = pd.read_csv(csv_input_path_2, header=None)
    
    # Concatenate vertically
    df_combined = pd.concat([df1, df2], ignore_index=True)
    
    # Shuffle dataframe rows and reset index
    df_shuffled = df_combined.sample(frac=1, random_state=421).reset_index(drop=True)
    
    # Select first 80% of rows
    train_rows = int(0.8 * len(df_shuffled))
    X_train = df_shuffled.iloc[:train_rows].values
    
    # Apply KMeans clustering
    kmeans = KMeans(n_clusters=n_centers, random_state=42)
    kmeans.fit(X_train)
    
    # Retrieve cluster centers
    #centers = kmeans.cluster_centers_
    closest, _ = pairwise_distances_argmin_min(kmeans.cluster_centers_, X_train)
    representative_points = X_train[closest]
    # Save centers to CSV using columns from input (if available)
    centers_df = pd.DataFrame(representative_points)
    centers_df.to_csv(csv_output_path, index=False, header=False)
    
    print(f"Saved {n_centers} cluster centers to {csv_output_path}")

#Example call
choose_k_centers(
    r"D:\DATASETS\datasets_EnCoD\encod_fragment\60k\enc.csv",
    r"D:\DATASETS\dataset_itctext + mnp\fragments\64117\all_encrar.csv",
    r".\enc_lcss_centers.csv",
    n_centers=4
)
