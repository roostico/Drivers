import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import re

combinationsDF = pd.read_parquet(
    "/Users/giovanniantonioni/IdeaProjects/Drivers/output/secondJobRDD/combination_data"
)

pairs = combinationsDF[["featureX", "featureY"]].drop_duplicates().values.tolist()
pdf = PdfPages("all_heatmaps.pdf")

def bin_sort_key(bin_label: str):
    label = bin_label.replace("%", "").replace("mph", "")

    if "+" in label:
        try:
            return float(label.replace("+", "")) + 1000
        except ValueError:
            return float("inf")  # fallback

    range_match = re.match(r"(\d+\.?\d*)-(\d+\.?\d*)", label)
    if range_match:
        return float(range_match.group(1))

    try:
        return float(label)
    except ValueError:
        return float("inf")

for featureX, featureY in pairs:
    sub = combinationsDF[
        (combinationsDF["featureX"] == featureX) &
        (combinationsDF["featureY"] == featureY)
        ]

    if sub.empty:
        continue

    x_bins = sorted(sub["binX"].unique(), key=bin_sort_key)
    y_bins = sorted(sub["binY"].unique(), key=bin_sort_key, reverse=True)

    pivot = sub.pivot(index="binY", columns="binX", values="avg_tip_pct")
    pivot = pivot.reindex(index=y_bins, columns=x_bins)

    # Plot
    plt.figure(figsize=(10, 8))
    sns.heatmap(pivot, annot=True, cmap="YlGnBu", fmt=".1f")
    plt.title(f"Avg Tip %: {featureX} vs {featureY}")
    plt.xlabel(featureX)
    plt.ylabel(featureY)
    plt.tight_layout()
    pdf.savefig()
    plt.close()

pdf.close()