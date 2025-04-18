import pandas as pd
import os
from dotenv import load_dotenv
from plot_utils import bin_label
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import re

load_dotenv()

result_dir = os.getenv('RESULTS_DIR')

# Get all parquet files in the result_dir
parquet_files = [
    os.path.join(result_dir, f)
    for f in os.listdir(result_dir)
    if f.endswith('.parquet')
]

files_number = len(parquet_files)
plot_per_row = 2
rows = int(np.ceil(files_number / plot_per_row))
# feat_name, step
relevant_features = [('distance', 10), ('time', 5)]
features_number = len(relevant_features)

# Create subplots
fig, axs = plt.subplots(rows, plot_per_row, figsize=(plot_per_row * 5, rows * 4), squeeze=False)

# Iterate and read each file
for idx, file_path in enumerate(parquet_files):
    df = pd.read_parquet(file_path)

    feature = df['feature'][0]

    df = df.dropna()
    df['value'] = pd.to_numeric(df['value'], errors='coerce')

    df_split = {}
    admissible_bins = set()

    for (feat_name, step) in relevant_features:
        # Add binned columns
        df[f'{feat_name}_bin'] = df[f'cost_{feat_name}_label'].apply(lambda x: bin_label(x, step=step))

        df_splitted = (
            df[['feature', 'value', 'count', f'{feat_name}_bin']]
            .groupby(['feature', 'value', f'{feat_name}_bin'], as_index=False)
            .agg({'count': 'sum'})
        )

        # Compute sum per ['feature', 'value']
        total_counts = df_splitted.groupby(['feature', 'value'])['count'].transform('sum')

        # Now compute the percentage per group
        df_splitted['pcg'] = round(df_splitted['count'] / total_counts * 100, 5)

        # df_splitted['pcg'] = round(df_splitted['count'] / df_splitted['count'].sum() * 100, 5)

        df_split[feat_name] = df_splitted

        admissible_bins.update(df_splitted[f'{feat_name}_bin'].dropna().unique())

    def parse_bin(bin_str):
        if bin_str.startswith('<'):
            return -1e6
        elif bin_str.startswith('>'):
            return 1e6
        else:
            match = re.search(r'\[(-?\d+)\|', bin_str)
            return int(match.group(1)) if match else np.nan

    sorted_bins = sorted(admissible_bins, key=parse_bin)

    bin_to_y = {bin_label: idx for idx, bin_label in enumerate(sorted_bins)}

    # Set up color palette
    colors = plt.get_cmap('tab10', features_number)

    ax = axs[idx // plot_per_row][idx % plot_per_row]
    ax.set_title(feature)
    ax.set_ylabel("Diff from avg price [%]")
    ax.set_xlabel("Value [%]" if 'pcg' in feature else 'Value')
    ax.set_yticks(range(len(sorted_bins)))

    step = max(step for _, step in relevant_features)

    visible_labels = [label if i == 0 or i == len(sorted_bins)-1 or i % step == 0 else '' for i, label in enumerate(sorted_bins)]
    ax.set_yticklabels(visible_labels)

    # Plotting loop
    for idx_features, (df_feat, df_splitted) in enumerate(df_split.items()):

        color = colors(idx_features % features_number)  # unique color per feature

        for _, row in df_splitted.iterrows():
            y = bin_to_y.get(row[f'{df_feat}_bin'], None)
            if y is not None:
                ax.scatter(
                    row['value'],
                    y,
                    s=10+row['pcg']*8,
                    color=color,
                    alpha=0.6
                )

# Hide unused subplots if any
for idx in range(files_number, rows * plot_per_row):
    fig.delaxes(axs[idx // plot_per_row][idx % plot_per_row])

fig.tight_layout()
plt.savefig(f"{os.path.join(result_dir + '/graphs.pdf')}", format='pdf')
plt.show()

