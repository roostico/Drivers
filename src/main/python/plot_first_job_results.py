import pandas as pd
import os
from dotenv import load_dotenv
from plot_utils import bin_label, bin_values, parse_bin
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import re
import matplotlib.patches as mpatches
from pyspark.sql import SparkSession
import argparse
import io
import s3fs

load_dotenv()

# Graphs per row
plot_per_row = 2

# Y ticks shown by step of this (i.e. 2 -> one label every 2 ticks)
step_y_ticks = 2

# Number of ticks on x axis of graps
max_bins_x = 15
# If needed to re-bin, set max value so last bin will be >= max_value bin
max_value_bin = 100

# feat_name, step
relevant_metrics = [('distance', 10), ('time', 10)]

# Plot points' size calculated as m*val + q
size_multiplier, size_scalar = 2, 6

parser = argparse.ArgumentParser(description="Parse arg for remote configurations.")
parser.add_argument('--remote', action='store_true', help='Read files from s3')
parser.add_argument('--dataset', help='Select dataset to work with', default=os.getenv("DATASET"))
parser.add_argument('--opt', action='store_true', help='Select rather optimized or not dataset to work with')

args = parser.parse_args()

if args.opt:
    output_dir = "output/firstJobOutputOpt"
else:
    output_dir = "output/firstJobOutput"

if args.remote:
    s3_path = f's3://{os.getenv("BUCKET")}/{output_dir}/{args.dataset}'
    df_all = pd.read_parquet(s3_path, engine='pyarrow')
else:
    result_dir = f'{os.getenv("OUTPUT_PATH")}/{output_dir}/{args.dataset}'

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
    df = feature_groups.get(feat_name)

    if df is None or df.empty:
        continue  # Skip features not present

    df = df.dropna()

    df_split = {}
    admissible_bins = set()

    for (metric_name, step) in relevant_metrics:

        # If more bins than max x ticks, need to map in new bins
        if len(df['value'].unique()) > max_bins_x:
            df = bin_values(df, max_bins_x, max_value_bin)

        # Add binned columns
        df[f'{metric_name}_bin'] = df[f'cost_{metric_name}_label'].apply(lambda x: bin_label(x, step=step))

        df_splitted = (
            df[['feature', 'value', 'count', f'{metric_name}_bin']]
            .groupby(['feature', 'value', f'{metric_name}_bin'], observed=True)
            .agg({'count': 'sum'})
        )

        # Compute sum per ['feature', 'value']
        total_counts = df_splitted.groupby(['feature', 'value'], observed=True)['count'].transform('sum')

        # Now compute the percentage per group
        df_splitted['pcg'] = round(df_splitted['count'] / total_counts * 100, 5)

        df_splitted = df_splitted[df_splitted['count'] != 0].reset_index()

        # Create a temporary column
        df_splitted['sort_key'] = df_splitted['value'].apply(parse_bin)

        # Sort using the computed values
        df_splitted = df_splitted.sort_values(by='sort_key')

        # Optionally drop the temp column
        df_splitted = df_splitted.drop(columns='sort_key')

        df_split[metric_name] = df_splitted

        admissible_bins.update(df_splitted[f'{metric_name}_bin'].dropna().unique())

    sorted_bins = sorted(admissible_bins, key=parse_bin)

    bin_to_y = {bin_label: idx for idx, bin_label in enumerate(sorted_bins)}

    # Set up color palette
    colors = plt.get_cmap('tab10', features_number)

    ax = axs[idx // plot_per_row][idx % plot_per_row]
    ax.set_title(feat_name)
    ax.set_ylabel("Diff from avg price [%]")
    ax.set_xlabel("Value [%]" if 'pcg' in feat_name else 'Value')
    ax.set_yticks(range(len(sorted_bins)))

    visible_labels = [label if i == 0 or i == len(sorted_bins) - 1 or i % step_y_ticks == 0 else '' for i, label in
                      enumerate(sorted_bins)]

    ax.set_yticklabels(visible_labels)

    legend_elements = []

    # Plotting loop
    for idx_features, (df_feat, df_splitted) in enumerate(df_split.items()):

        color = colors(idx_features % features_number)  # unique color per feature

        legend_elements.append(
            mpatches.Patch(color=color, label=df_feat)
        )

        for _, row in df_splitted.iterrows():
            y = bin_to_y.get(row[f'{df_feat}_bin'], None)
            if y is not None:
                x_val = row['value']
                if isinstance(x_val, float) and x_val.is_integer():
                    x_val = int(x_val)
                ax.scatter(
                    x_val,
                    y,
                    s=size_scalar + row['pcg'] * size_multiplier,
                    color=color,
                    alpha=0.6
                )

        # line "0"
        ymin, ymax = ax.get_ylim()
        middle_y = (ymin + ymax) / 2
        ax.axhline(middle_y, color='gray', linestyle='--', linewidth=1)

    ax.legend(
        handles=legend_elements,
        title="Avg cost by",
        bbox_to_anchor=(1.05, 1),
        loc='upper left',
        borderaxespad=0.
    )


    def clean_label(label):
        try:
            num = float(label)
            if num.is_integer():
                return str(int(num))
            return str(num)
        except ValueError:
            return label  # keep bin labels or non-numeric ones as they are


    ticks = ax.get_xticks()
    cleaned_labels = [clean_label(lbl.get_text()) for lbl in ax.get_xticklabels()]
    if len(cleaned_labels) > 10 or any(len(str(label)) > 5 for label in cleaned_labels):
        ax.set_xticks(ticks)
        ax.set_xticklabels(cleaned_labels, rotation=315, fontsize=8)
    else:
        ax.set_xticks(ticks)
        ax.set_xticklabels(cleaned_labels, fontsize=8)

# Hide unused subplots if any
# for idx in range(files_number, rows * plot_per_row):
#    fig.delaxes(axs[idx // plot_per_row][idx % plot_per_row])

fig.tight_layout()
plt.subplots_adjust(right=0.8)

result_file_name = 'graphs.pdf'

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
