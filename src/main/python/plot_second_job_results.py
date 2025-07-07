import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import sys
import os
from itertools import combinations
from matplotlib.backends.backend_pdf import PdfPages
import math
import re
import s3fs

def load_parquet(parquet_dir):
    print(parquet_dir)
    if parquet_dir.startswith("s3://"):
        print(f"Loading Parquet from S3: {parquet_dir}")
    else:
        print(f"Loading Parquet from local path: {parquet_dir}")

    df = pd.read_parquet(parquet_dir)
    return df

def extract_dataset_name(parquet_dir):
    path = parquet_dir.replace("s3://", "")
    parts = os.path.normpath(path).split(os.sep)
    if len(parts) >= 3:
        return parts[-3]
    else:
        return "dataset"


def bin_sort_key(bin_label):
    if pd.isna(bin_label):
        return float('-inf')
    bin_label = bin_label.strip()
    match = re.search(r'\d+(\.\d+)?', bin_label)
    if match:
        num = float(match.group())
        return -num
    else:
        return 0


def generate_pairwise(df, output_pdf):
    feature_types = df['feature_type'].unique()
    feature_pairs = list(combinations(feature_types, 2))

    num_pairs = len(feature_pairs)
    num_cols = 2
    num_rows = math.ceil(num_pairs / num_cols)

    with PdfPages(output_pdf) as pdf:
        fig, axes = plt.subplots(num_rows, num_cols, figsize=(14, num_rows * 6))
        if num_rows == 1:
            axes = [axes]

        for idx, (featureA, featureB) in enumerate(feature_pairs):
            dfA = df[df['feature_type'] == featureA][['bin', 'avg_tip_pct']].rename(
                columns={'bin': 'binA', 'avg_tip_pct': 'tipA'}
            )
            dfB = df[df['feature_type'] == featureB][['bin', 'avg_tip_pct']].rename(
                columns={'bin': 'binB', 'avg_tip_pct': 'tipB'}
            )

            dfA['key'] = 1
            dfB['key'] = 1
            cross = pd.merge(dfA, dfB, on='key').drop('key', axis=1)
            cross['diff'] = cross['tipA'] - cross['tipB']

            pivot = cross.pivot(index='binB', columns='binA', values='diff').dropna(axis=0, how='all').dropna(axis=1, how='all')
            sorted_cols = sorted(pivot.columns, key=bin_sort_key)
            sorted_idx = sorted(pivot.index, key=bin_sort_key)
            sorted_cols = list(reversed(sorted_cols))
            pivot = pivot.loc[sorted_idx, sorted_cols]

            row = idx // num_cols
            col = idx % num_cols
            ax = axes[row][col] if num_rows > 1 else axes[col]

            sns.heatmap(
                pivot, annot=True, fmt=".2f", cmap='RdBu_r', center=0,
                linewidths=0.5, linecolor='gray', ax=ax
            )
            ax.set_title(f"Tip % Diff: {featureA} vs {featureB}")
            ax.set_xlabel(f"{featureA} bins")
            ax.set_ylabel(f"{featureB} bins")

        for idx in range(num_pairs, num_rows * num_cols):
            row = idx // num_cols
            col = idx % num_cols
            ax = axes[row][col] if num_rows > 1 else axes[col]
            ax.axis('off')

        plt.tight_layout()
        pdf.savefig(fig)
        plt.close(fig)

    print(f"Pairwise feature heatmaps saved to {output_pdf}")

def generate_same_feature(df, output_pdf):
    feature_types = df['feature_type'].unique()
    num_features = len(feature_types)
    num_cols = 2
    num_rows = math.ceil(num_features / num_cols)

    with PdfPages(output_pdf) as pdf:
        fig, axes = plt.subplots(num_rows, num_cols, figsize=(14, num_rows * 6))
        if num_rows == 1:
            axes = [axes]

        for idx, feature in enumerate(feature_types):
            dfA = df[df['feature_type'] == feature][['bin', 'avg_tip_pct']].rename(
                columns={'bin': 'binA', 'avg_tip_pct': 'tipA'}
            )
            dfB = df[df['feature_type'] == feature][['bin', 'avg_tip_pct']].rename(
                columns={'bin': 'binB', 'avg_tip_pct': 'tipB'}
            )

            dfA['key'] = 1
            dfB['key'] = 1
            cross = pd.merge(dfA, dfB, on='key').drop('key', axis=1)
            cross['diff'] = cross['tipA'] - cross['tipB']

            pivot = cross.pivot(index='binB', columns='binA', values='diff').dropna(axis=0, how='all').dropna(axis=1, how='all')
            sorted_cols = sorted(pivot.columns, key=bin_sort_key)
            sorted_idx = sorted(pivot.index, key=bin_sort_key)
            sorted_cols = list(reversed(sorted_cols))
            pivot = pivot.loc[sorted_idx, sorted_cols]

            row = idx // num_cols
            col = idx % num_cols
            ax = axes[row][col] if num_rows > 1 else axes[col]

            sns.heatmap(
                pivot, annot=True, fmt=".2f", cmap='RdBu_r', center=0,
                linewidths=0.5, linecolor='gray', ax=ax
            )
            ax.set_title(f"Tip % Diff within {feature}")
            ax.set_xlabel(f"{feature} bins (X)")
            ax.set_ylabel(f"{feature} bins (Y)")

        for idx in range(num_features, num_rows * num_cols):
            row = idx // num_cols
            col = idx % num_cols
            ax = axes[row][col] if num_rows > 1 else axes[col]
            ax.axis('off')

        plt.tight_layout()
        pdf.savefig(fig)
        plt.close(fig)

    print(f"Same-feature heatmaps saved to {output_pdf}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python generate_feature_heatmaps.py /path/to/tip_avg_per_bin/all_features [pair|same]")
        sys.exit(1)

    parquet_dir = sys.argv[1]
    mode = sys.argv[2] if len(sys.argv) > 2 else 'pair'

    dataset_name = extract_dataset_name(parquet_dir)
    print(f"Dataset name extracted: {dataset_name}")

    if parquet_dir.startswith("s3://") and not parquet_dir.endswith("/"):
        parquet_dir += "/"

    df = load_parquet(parquet_dir)
    df[['feature_type', 'bin']] = df['feature'].str.split('_', n=1, expand=True)

    if mode == 'pair':
        output_pdf = f"diff_feature_comparison_{dataset_name}.pdf"
        generate_pairwise(df, output_pdf)
    elif mode == 'same':
        output_pdf = f"same_feature_comparison_{dataset_name}.pdf"
        generate_same_feature(df, output_pdf)
    else:
        print(f"Unknown mode '{mode}', use 'pair' or 'same'")
        sys.exit(1)

if __name__ == "__main__":
    main()
