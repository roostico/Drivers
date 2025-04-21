import re
import numpy as np
import pandas as pd

def bin_label(label, step):
    if label:
        # Extract numeric values
        nums = re.findall(r"-?\d+", label)
        if not nums:
            return "unknown"
        val = int(nums[0])

        if val <= -100:
            return "<= -100"
        elif val >= 100:
            return ">= 100"
        else:
            start = (val // step) * step
            return f"[{start}|{start + step})"
    else:
        return None

def parse_bin(bin_str):
    if bin_str.startswith('<'):
        return -1e6
    elif bin_str.startswith('>'):
        return 1e6
    else:
        match = re.search(r'\[(-?\d+(?:\.\d+)?)\|', bin_str)
        return float(match.group(1)) if match else bin_str

def extract_midpoint(interval_str):
    match = re.match(r'\[(\d+)\|(\d+)\)', interval_str)
    if match:
        if match.lastindex and match.lastindex >= 2:
            start = int(match.group(1))
            end = int(match.group(2))
            return (start + end) / 2
        else:
            return int(match.group(1))
    return interval_str  # handle parsing errors

def bin_values(df, max_bins_x, max_value=100):
    values = df['value'].unique()

    df['midpoint'] = df['value'].apply(extract_midpoint)

    sorted_values = sorted(values, key=parse_bin)

    match_min = re.search(r'\[(-?\d+)\|', sorted_values[0])
    match_max = re.search(r'\[(-?\d+)\|', sorted_values[-1])
    min_val = float(match_min.group(1))
    if match_max:
        max_val = match_max.group(2) if match_max.lastindex and match_max.lastindex >= 2 else match_max.group(1)
        max_val = min(max_value, float(max_val))
    else:
        max_val=max_value

    range_val = np.linspace(min_val, max_val, max_bins_x)
    range_val = [int(x) for x in range_val]

    bins_val = [f'[{x}|{range_val[idx+1]})' if x != range_val[idx+1] else '' for idx, x in enumerate(range_val[:-1])]

    bins_val.append(f'>={range_val[-1]}')
    range_val.append(float('inf'))

    df['value'] = pd.cut(df['midpoint'], bins=range_val, labels=bins_val, right=False, include_lowest=True)

    headers_with_no_count = list(df.columns)
    headers_with_count = list(df.columns)
    for col in ['count', 'midpoint', 'pcg']:
        headers_with_no_count.remove(col)
    for col in ['midpoint', 'pcg']:
        headers_with_count.remove(col)

    new_df = (
        df[headers_with_count]
        .groupby(headers_with_no_count, observed=True)
        .agg({'count': 'sum'})
    )

    new_df = new_df[new_df['count'] != 0]

    total_counts = new_df.groupby(['feature', 'value'], observed=True)['count'].transform('sum')
    new_df['pcg'] = round(new_df['count'] / total_counts * 100, 5)

    return new_df.reset_index()
