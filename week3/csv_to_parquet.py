import polars as pl
from datetime import datetime
import pyarrow as pa 
import pyarrow.parquet as pq


def main():
    # Read CSV File into Polars LazyFrame
    df = pl.scan_csv("2022_place_canvas_history.csv")
    # df = pl.scan_csv("bigger_sample.csv")
    # Convert Timestamp to Datetime Format
    # Remove the ms and 'UTC' from timestamp
    df = df.with_columns(
        (pl.col("timestamp").str.slice(0, length=16)).str.to_datetime(
            "%Y-%m-%d %H:%M"))
    # Convert to date format "%Y-%m-%d %H:%M"
    # df = df.with_columns(pl.col("timestamp").str.to_datetime("%Y-%m-%d %H:%M"))
    print("Splicing done")
    # df = df.collect()
    # Next task: Create a dictionary to map usernames to IDs
    #mapping = {}
    #id = 1
    user_id_only_df = df.select("user_id").unique().collect()
    mapping = {user_id: id for id, user_id in enumerate(
        user_id_only_df["user_id"], start=1)}
    print("mapping done")
    # Replace user_id long string with dict
    df = df.with_columns(
        pl.col("user_id").replace(mapping))
    print("replace done")

    
    # Write to parquet file using snappy compression
    df = df.collect()
    df.write_parquet("new_dataset.parquet", compression="snappy")
    return 0

if __name__ == '__main__':
    main()
