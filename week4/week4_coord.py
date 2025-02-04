import polars as pl
from datetime import datetime
import pyarrow as pa 
import pyarrow.parquet as pq
import sys
from time import perf_counter_ns

def pixel_count_by_color(df):
    df2 = df.group_by("pixel_color").len(
        name="count").sort(
            "count", descending=True)
    print(df2)
    return
def unique(df):
    # Purpose: Count Distinct of Users Placing Pixel
    return df.n_unique()
def main():
    # Start Timer
    time_start = perf_counter_ns()
    # Pull just the pixel coord for all time
    df = pl.scan_parquet("new_dataset.parquet").select(
        ["coordinate"]
    ).collect()
    # Count pixel amount for each coord
    df = df.group_by(
        "coordinate").len(name="coord_count").sort(
            "coord_count", descending=True)

    # For total users
    user_df = pl.scan_parquet("new_dataset.parquet").select(
        ["user_id"]
    ).collect()
    print(user_df.head())
    total_user_count = user_df.n_unique()
    print("Total Users:", total_user_count)
    
    # For Tableau CSV, times of 0,0 Placement
    start_df = pl.scan_parquet("new_dataset.parquet").filter(
        (pl.col("coordinate") == "0,0")).select(
        ["timestamp"]
    ).collect()
    start_df.write_csv("start_pixel_over_time.csv")
    
    # For Tableau CSV, count of different colors at both pixels
    eyes_df = pl.scan_parquet("new_dataset.parquet").filter(
        (pl.col("coordinate") == "359,564") |
        (pl.col("coordinate") == "349,564")).select(
            ["pixel_color"]).collect()
    eyes_df = (eyes_df.group_by("pixel_color").len(
        name="count")).sort(
            "count", descending=True
        )
    print(eyes_df.head())
    
    # Top 3 Pixel Dataset
    specific_df = pl.scan_parquet("new_dataset.parquet").filter(
        ((pl.col("coordinate") == "0,0") |
        (pl.col("coordinate") == "359,564") |
        (pl.col("coordinate") == "349,564"))
    ).collect()
    print(specific_df.head())
    # Calculate Number of Distinct Users who Placed (0,0)
    print("Distinct Users at (0,0):", 
        unique((specific_df.filter(
            (pl.col("coordinate") == "0,0")).select(
                "user_id"))
        )
    )
    

    # Print top 3 coords
    print(df.head(3))
    # Stop Timer
    stop = perf_counter_ns()
    print("Time:", (stop-time_start) / 1000000, 'ms')
    return 0
if __name__ == '__main__':
    main()
