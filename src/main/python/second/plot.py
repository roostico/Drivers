import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import re




def bin_sort_key(bin_label):
    bin_label = str(bin_label)
    label = bin_label.replace("%", "").replace("mph", "")

    if "+" in label:
        try:
            return float(label.replace("+", "")) + 1000
        except ValueError:
            return float("inf")

    range_match = re.match(r"(\d+\.?\d*)-(\d+\.?\d*)", label)
    if range_match:
        return float(range_match.group(1))

    try:
        return float(label)
    except ValueError:
        return float("inf")

def diff_matrix_heatmap():

    df = pd.read_parquet(
        "/Users/giovanniantonioni/IdeaProjects/Drivers/output/secondJobRDD/tip_avg_per_bin/all_features"
    )

    pdf = PdfPages("tip_difference_matrices_all_features.pdf")
    features = df["feature"].unique()

    for feature in features:
        feature_df = df[df["feature"] == feature].copy()

        feature_df = feature_df.set_index("bin").sort_index(key=lambda idx: [bin_sort_key(b) for b in idx])

        if feature_df.empty or len(feature_df) < 2:
            continue

        diff_matrix = feature_df["avg_tip_pct"].values[:, None] - feature_df["avg_tip_pct"].values[None, :]
        diff_df = pd.DataFrame(diff_matrix, index=feature_df.index, columns=feature_df.index)
        diff_df = diff_df.iloc[::-1]

        plt.figure(figsize=(10, 8))
        sns.heatmap(diff_df, annot=True, center=0, cmap="coolwarm", fmt=".2f")
        plt.title(f"Pairwise Tip % Difference: {feature}")
        plt.xlabel("Compared To")
        plt.ylabel("Reference Bin")
        plt.tight_layout()
        pdf.savefig()
        plt.close()

    pdf.close()


def heatmaps():
    combinationsDF = pd.read_parquet(
        "/Users/giovanniantonioni/IdeaProjects/Drivers/output/secondJobRDD/combination_data"
    )

    pairs = combinationsDF[["featureX", "featureY"]].drop_duplicates().values.tolist()
    pdf = PdfPages("all_heatmaps.pdf")

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

if __name__ == "__main__":
    diff_matrix_heatmap()
    heatmaps()