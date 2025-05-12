import pandas as pd
import os
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import re
from pyspark.sql import SparkSession
import argparse
import s3fs
import matplotlib.pyplot as plt
import seaborn as sns
import math

load_dotenv()

# Graphs per row
plot_per_row = 2

# Y ticks shown by step of this (i.e. 2 -> one label every 2 ticks)
step_y_ticks = 2

parser = argparse.ArgumentParser(description="Parse arg for remote configurations.")
parser.add_argument('--remote', action='store_true', help='Read files from s3')

args = parser.parse_args()

if args.remote:
    s3_path = f's3://{os.getenv("BUCKET")}/{os.getenv("OUTPUT_DIR")}/{os.getenv("DATASET")}'
    df_all = pd.read_parquet(s3_path, engine='pyarrow')
else:
    result_dir = f'{os.getenv("OUTPUT_PATH")}/{os.getenv("OUTPUT_DIR")}/{os.getenv("DATASET")}'

    parquet_files = [
        os.path.join(result_dir, f)
        for f in os.listdir(result_dir)
        if f.endswith('.parquet')
    ]

    # Load and concatenate all files
    df_all = pd.concat([pd.read_parquet(fp) for fp in parquet_files], ignore_index=True)

# Drop NA globally (or inside loop if needed)
df_all = df_all.dropna()

relevant_features = df_all['feature'].unique()
relevant_features.sort()

# Group all dataframes by feature value
feature_groups = {
    feat: df_all[df_all['feature'] == feat].copy()
    for feat in relevant_features
}

features_number = len(relevant_features)
rows = int(np.ceil(features_number / plot_per_row))
fig, axs = plt.subplots(rows, plot_per_row, figsize=(plot_per_row * 5, rows * 4), squeeze=False)

for idx, feat_name in enumerate(relevant_features):
    df = feature_groups.get(feat_name).sort_values(by=['value'])

    # Filter and get avg_tip values
    sub_df = df[df['feature'] == feat_name].set_index('value')['avg_tip']
    values = sub_df.index
    tips = sub_df.values

    # Compute percentage difference matrix
    perc_diff_matrix = ((tips[:, None] - tips[None, :]) / tips[None, :]) * 100
    perc_diff_df = pd.DataFrame(perc_diff_matrix, index=values, columns=values)

    # sorted_bins = sorted(df['value'].unique(), key=parse_bin)

    row, col = divmod(idx, plot_per_row)
    ax = axs[row][col]
    ax.set_title(f"Pairwise Avg Tip Difference for {feat_name}")
    ax.set_ylabel("Average tip difference [%]")
    ax.set_xlabel("Value [%]" if 'pcg' in feat_name else 'Value')
    # ax.set_yticks(range(len(sorted_bins)))
    sns.heatmap(perc_diff_df, annot=True, fmt=".1f", cmap='coolwarm', center=0, ax=ax, cbar_kws={"label": "% Diff"})

# Hide any unused subplots
for idx in range(features_number, rows * plot_per_row):
    row, col = divmod(idx, plot_per_row)
    axs[row][col].axis('off')

fig.tight_layout()
plt.subplots_adjust(right=0.8)

result_file_name = 'graphs_2nd.pdf'

if args.remote:
    # Save to in-memory buffer
    buf = io.BytesIO()
    plt.savefig(buf, format='pdf')
    buf.seek(0)

    # Upload to S3
    fs = s3fs.S3FileSystem()
    with fs.open(f'{s3_path}/{result_file_name}', 'wb') as f:
        f.write(buf.read())
else:
    plt.savefig(f"{os.path.join(f'{result_dir}/{result_file_name}')}", format='pdf')

plt.show()
