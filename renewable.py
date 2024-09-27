import pandas as pd
from scipy import stats
import numpy as np
import sqlite3
import os

# Function to clean data: handle missing values and remove outliers
def clean_data(df):
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    # Use forward fill for missing values
    df.ffill(inplace=True)
    df.fillna(df.mean(numeric_only=True), inplace=True)
    # Remove outliers using Z-score; keep rows within 3 standard deviations
    df = df[(np.abs(stats.zscore(df[['wind_speed', 'power_output']])) < 3).all(axis=1)]
    return df


# Function to calculate summary statistics for each turbine
def calculate_statistics(df):
    return df.groupby('turbine_id').agg(
        min_output=('power_output', 'min'),
        max_output=('power_output', 'max'),
        avg_output=('power_output', 'mean')
    ).reset_index()

# Function to detect anomalies based on output deviation
def detect_anomalies(df):
    anomalies = []
    for turbine_id in df['turbine_id'].unique():
        turbine_data = df[df['turbine_id'] == turbine_id]
        mean_output = turbine_data['power_output'].mean()
        std_dev = turbine_data['power_output'].std()
        anomaly_df = turbine_data[(turbine_data['power_output'] < mean_output - 2 * std_dev) |
                                  (turbine_data['power_output'] > mean_output + 2 * std_dev)]
        anomalies.append(anomaly_df)
    return pd.concat(anomalies) if anomalies else pd.DataFrame()

# Define paths
data_folder = "C:\\Users\\ajayk\\Desktop\\windmil"  # Update this path to match your setup
db_path = "turbine_data.db"

# Process files
all_data = pd.concat([clean_data(pd.read_csv(os.path.join(data_folder, f)))
                      for f in os.listdir(data_folder) if f.endswith('.csv')])

# Calculate statistics and detect anomalies
summary_stats = calculate_statistics(all_data)
anomalies_detected = detect_anomalies(all_data)

# Store in SQLite database
conn = sqlite3.connect(db_path)
all_data.to_sql('cleaned_data', conn, if_exists='replace', index=False)
summary_stats.to_sql('summary_statistics', conn, if_exists='replace', index=False)
anomalies_detected.to_sql('anomalies', conn, if_exists='replace', index=False)
conn.close()

# Display results
print("Summary Statistics:")
print(summary_stats.head())
print(f"Number of Anomalies Detected: {anomalies_detected.shape[0]}")
