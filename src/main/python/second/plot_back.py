import pandas as pd
import seaborn as sns
import os
import matplotlib.pyplot as plt

df = pd.read_parquet(
    "/Users/giovanniantonioni/IdeaProjects/Drivers/output/secondJobRDD/allCombinationsImpact/part-00000-f1907096-1b69-4e5c-9f04-b39e692d68dd-c000.snappy.parquet"
)
sns.set(style="whitegrid")

df['avg_tip'] = pd.to_numeric(df['avg_tip'], errors='coerce')
df['avg_duration'] = pd.to_numeric(df['avg_duration'], errors='coerce')
df.dropna(subset=['avg_tip', 'avg_duration'], inplace=True)


print("Preview:")
print(df.head())
print("\nSchema:")
print(df.dtypes)



heatmap_data = df.pivot_table(
    index="trip_duration_min_bin",
    columns="trip_distance_bin",
    values="avg_tip",
    aggfunc="mean"
)

plt.figure(figsize=(10, 6))
sns.heatmap(heatmap_data, annot=True, fmt=".1f", cmap="YlGnBu")
plt.title("Average Tip by Trip Duration and Distance")
plt.xlabel("Trip Distance Bin")
plt.ylabel("Trip Duration Bin")
plt.tight_layout()
plt.savefig("avg_tip_by_duration_distance.png", dpi=300, bbox_inches="tight")
plt.close()
