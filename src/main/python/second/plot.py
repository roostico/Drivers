import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import itertools
import os

sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)

all_combinations = pd.read_parquet(
    "/Users/giovanniantonioni/IdeaProjects/Drivers/output/secondJobRDD/allCombinationsImpact/part-00000-5a7e9080-9e93-48be-82fb-8963cbbcd30e-c000.snappy.parquet"
)

rush_hour =  pd.read_parquet(
    "/Users/giovanniantonioni/IdeaProjects/Drivers/output/secondJobRDD/rushHourAnalysis/part-00000-8b02c536-f657-4299-85dc-38b40f3129aa-c000.snappy.parquet"
)

monthly_pattern = pd.read_parquet(
    "/Users/giovanniantonioni/IdeaProjects/Drivers/output/secondJobRDD/monthlyPattern/part-00000-d5745e96-fd61-47dd-8aba-b3145c26bb0f-c000.snappy.parquet"
)


BIN_COLUMNS = [
    "fare_amount_bin",
    "trip_distance_bin",
    "trip_duration_min_bin",
    "tip_percentage_bin",
    "speed_mph_bin",
    "trip_hour_bucket"
]

def plot_heatmap(data, x_bin: str, y_bin: str,
                 x_order: list[str] = None, y_order: list[str] = None,
                 output_filename: str = None):
    """
    Create a heatmap showing how avg_tip varies by two categorical bins.

    Parameters:
    - data: DataFrame containing the relevant data
    - x_bin: name of the column to use on the x-axis (columns of the heatmap)
    - y_bin: name of the column to use on the y-axis (rows of the heatmap)
    - x_order: optional list specifying the desired order of x_bin categories
    - y_order: optional list specifying the desired order of y_bin categories
    - output_filename: optional filename for saving the plot
    """
    data = data.dropna()
    heatmap_data = data.pivot_table(
        values='avg_tip',
        index=y_bin,
        columns=x_bin,
        aggfunc='mean'
    )

    if y_order:
        heatmap_data = heatmap_data.reindex(y_order, axis=0)
    if x_order:
        heatmap_data = heatmap_data.reindex(x_order, axis=1)

    heatmap_data = heatmap_data.apply(pd.to_numeric, errors='coerce').fillna(0)

    plt.figure(figsize=(10, 8))
    cmap = sns.color_palette("YlGnBu", as_cmap=True)
    ax = sns.heatmap(heatmap_data, annot=True, fmt=".2f", cmap=cmap)
    plt.title(f'Average Tip Amount by {y_bin} and {x_bin}', fontsize=16)
    plt.xlabel(x_bin)
    plt.ylabel(y_bin)
    plt.tight_layout()

    if output_filename:
        plt.savefig(output_filename, dpi=300)
    else:
        plt.show()

def plot_tips_by_time_of_day():
    """Create a bar plot showing average tip by time bucket"""
    time_tip_data = all_combinations.groupby('trip_hour_bucket')['avg_tip'].mean().reset_index()

    time_order = ['morning', 'midday', 'evening', 'night', 'late_night']
    time_tip_data['trip_hour_bucket'] = pd.Categorical(
        time_tip_data['trip_hour_bucket'],
        categories=time_order,
        ordered=True
    )
    time_tip_data = time_tip_data.sort_values('trip_hour_bucket')

    plt.figure(figsize=(10, 6))
    ax = sns.barplot(x='trip_hour_bucket', y='avg_tip', data=time_tip_data, palette='viridis')

    for i, p in enumerate(ax.patches):
        ax.annotate(f'${p.get_height():.2f}',
                    (p.get_x() + p.get_width() / 2., p.get_height()),
                    ha='center', va='bottom', fontsize=12)

    plt.title('Average Tip by Time of Day', fontsize=16)
    plt.xlabel('Time of Day')
    plt.ylabel('Average Tip Amount ($)')
    plt.tight_layout()
    plt.savefig('tips_by_time_of_day.png', dpi=300)

def plot_rush_hour_comparison():
    """Create a grouped bar chart comparing metrics during rush hour vs non-rush hour"""
    rush_hour_melted = rush_hour.melt(
        id_vars=['is_rush_hour'],
        value_vars=['avg_duration', 'avg_tip_pct', 'avg_speed'],
        var_name='metric',
        value_name='value'
    )

    rush_hour_melted['is_rush_hour'] = rush_hour_melted['is_rush_hour'].map(
        {True: 'Rush Hour', False: 'Non-Rush Hour'}
    )

    plt.figure(figsize=(12, 7))
    ax = sns.barplot(x='metric', y='value', hue='is_rush_hour', data=rush_hour_melted, palette='Set2')

    ax.set_xticklabels(['Avg. Duration (min)', 'Avg. Tip (%)', 'Avg. Speed (mph)'])
    plt.title('Comparison of Metrics: Rush Hour vs. Non-Rush Hour', fontsize=16)
    plt.xlabel('')
    plt.ylabel('Value')
    plt.legend(title='')

    for i, p in enumerate(ax.patches):
        height = p.get_height()
        ax.text(p.get_x() + p.get_width()/2., height + 0.1, f'{height:.2f}',
                ha='center', fontsize=10)

    plt.tight_layout()
    plt.savefig('rush_hour_comparison.png', dpi=300)

def plot_monthly_tip_trends():
    """Create a line plot showing monthly trends in tips over time"""
    month_names = {
        0: 'Jan', 1: 'Feb', 2: 'Mar', 3: 'Apr', 4: 'May', 5: 'Jun',
        6: 'Jul', 7: 'Aug', 8: 'Sep', 9: 'Oct', 10: 'Nov', 11: 'Dec'
    }
    monthly_pattern['month_name'] = monthly_pattern['month'].map(month_names)

    monthly_pattern['date'] = pd.to_datetime(
        monthly_pattern['year'].astype(str) + '-' +
        (monthly_pattern['month'] + 1).astype(str) + '-01'
    )
    monthly_pattern = monthly_pattern.sort_values('date')

    plt.figure(figsize=(14, 7))
    ax = sns.lineplot(
        x='date',
        y='avg_tip',
        data=monthly_pattern,
        marker='o',
        linewidth=2,
        color='royalblue'
    )

    # Add a secondary axis for trip count
    ax2 = ax.twinx()
    sns.lineplot(
        x='date',
        y='total_trips',
        data=monthly_pattern,
        marker='s',
        linestyle='--',
        linewidth=1.5,
        color='darkred',
        alpha=0.7,
        ax=ax2
    )

    # Format axes
    ax.set_xlabel('')
    ax.set_ylabel('Average Tip Amount ($)', color='royalblue')
    ax2.set_ylabel('Total Trips', color='darkred')
    ax.tick_params(axis='y', colors='royalblue')
    ax2.tick_params(axis='y', colors='darkred')

    # Format x-axis dates
    plt.gcf().autofmt_xdate()

    plt.title('Monthly Trend: Average Tips and Trip Volume', fontsize=16)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('monthly_tip_trends.png', dpi=300)


if __name__ == "__main__":
    # outputDirHeatmap = "heatmap"
    # os.makedirs(outputDirHeatmap, exist_ok=True)
    #
    # all_combinations = all_combinations.dropna()
    # all_combinations = all_combinations[all_combinations["avg_tip"] > 0]
    #
    # for y_bin, x_bin in itertools.combinations(BIN_COLUMNS, 2):
    #
    #     output_file = os.path.join(
    #         outputDirHeatmap, f'tip_heatmap_{y_bin}_vs_{x_bin}.png'
    #     )
    #
    #     plot_heatmap(
    #         all_combinations,
    #         x_bin=x_bin,
    #         y_bin=y_bin,
    #         output_filename=output_file
    #     )
    #     print(f"Saved heatmap: {output_file}")
    #
    # plot_tips_by_time_of_day()


    plot_heatmap(
        all_combinations,
        x_bin="fare_amount_bin",
        y_bin="fare_amount_bin",
        output_filename="test_fare_amount"
    )

   #plot_rush_hour_comparison()
   #plot_monthly_tip_trends()