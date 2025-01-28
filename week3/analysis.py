import polars as pl
from datetime import datetime
import pyarrow as pa 
import pyarrow.parquet as pq
import sys
from time import perf_counter_ns

# DONE Purpose: Rank colors by distinct users placing colors
def color_ranking(df):
    color_dict = {
        "#FF4500": "Red", "#FFA800": "Orange", "FFD635": "Yellow",
        "#00A368": "Dark Green", "#7EED56": "Light Green",
        "#2450A4": "Dark Blue", "#2690EA": "Blue",
        "#51E9F4": "Light Blue", "#811E9F": "Dark Purple",
        "#B44AC0": "Purple", "#FF99AA": "Light Pink",
        "#9C6926": "Brown", "#FFFFFF": "White", "#D4D7D9": "Light Gray",
        "#898D90": "Gray", "#000000": "Black", "#FF3881": "Pink",
        "#00756F": "Dark Teal", "#493AC1": "Indigo",
        "#009EAA": "Teal", "#00CC78": "Green", "#6D482F": "Dark Brown",
        "#6D001A": "Burgandy", "#FFF8D8": "Pale Yellow",
        "#00CCC0": "Light Teal", "#94B3FF": "Lavender",
        "#E4ABFF": "Light Purple", "#DE107F": "Magenta",
        "#FFB470": "Beige", "#515252": "Dark Gray", "#6A5CFF": "Dark Lavender"
    }
    # Get the Distinct Count of Users for Each Color
    color_count_df = df.group_by("pixel_color").n_unique()
    # Convert hex code to english, only return count and english,
    # order by desc
    return ((color_count_df.with_columns(
        pl.col("pixel_color").map_elements(
        lambda x: color_dict.get(x)).alias("eng_colors"))).select(
            ["user_id", "eng_colors"])).sort(["user_id"], descending=True)
    
# Purpose: Average session length where user has 1+ pixel placement
# during time period, (session = activity within 15min window of inactivity)
def avg_session_length(df): # NEEDS WORK
    # DF has columns timestamp and user_id
    # Sort by user and timestamp
    df = df.sort(["user_id", "timestamp"])
    
    # shift by 1
    df = df.with_columns(
        (pl.col("timestamp").shift(1).over(
            "user_id").alias(
                "shift1")))
   
    # Subtract
    df = df.with_columns(
        (pl.col("timestamp") - pl.col("shift1")).alias(
            "diff")
    )
    # Label a new session every time next val is > 15 mins away.
    df = df.with_columns(
        pl.when(
            (pl.col("diff") > pl.duration(minutes=15)) |
            (pl.col("diff").is_null())
        ).then(
            pl.lit("new_session")
        ).otherwise(
            pl.lit("same_session")
        ).alias("session_test")
    )
    # Add a column that converts session_test to session_id
    df = df.with_columns(
        (pl.col("session_test") == pl.lit("new_session")).cum_sum()
    )
    # Remove rows where count(session_test) = 1
    df = df.filter(
        (pl.len() > 1).over("session_test")
    )
    
    # Calculate duration for each session
    df = df.with_columns(
        (pl.col("timestamp").max() - 
         pl.col("timestamp").min()).over("session_test").alias(
             "session_duration"
         )
    )
    # Keep only 1 row for each session
    df = df.unique(subset="session_test")
    # Sum session_duration and divide by number of rows
    session_sum = df.select(
        ((pl.col("session_duration").sum()) /
            pl.col("session_test").len()).alias("avg")
    )
    return session_sum.item(0, "avg")

# Purpose: 50th, 75th, 90th, 99th percentiles of # pixels placed
def pixel_placement(df): 
    # Data frame: just 1 col containing # of pixels for each user
    pixel_count_df = ((df.group_by("user_id").len(name="pixel_count")).sort(
        ["pixel_count"], descending=False)).select(
            ["pixel_count"])
    # Get total count
    total_values = pixel_count_df.select(pl.len()).item()
    # Percentile calculation note: Get index given percentile
                                # and then the value at that index
    # 50th Percentile
    percentile50 = pixel_count_df.item(
                    round(0.5 * (total_values + 1)) - 1, "pixel_count")
    # 75th Percentile
    percentile75 = pixel_count_df.item(
                    round(0.75 * (total_values + 1)) - 1, "pixel_count")
    # 90th Percentile
    percentile90 = pixel_count_df.item(
                    round(0.9 * (total_values + 1) - 1), "pixel_count")
    # 99th Percentile
    percentile99 = pixel_count_df.item(
                    round(0.99 * (total_values + 1) - 1), "pixel_count")
    return [percentile50, percentile75, percentile90, percentile99]
# Purpose: Return Count of Users who placed first pixel ever within timeframe
def first_time_users(df, start, end):
    # print("In first_time users")
    # Get the first pixel timestamp for each user
    df = df.with_columns(
        pl.col("timestamp").min().over("user_id").alias("first_pixel_time")
    )
    # Filter to only get first pixel in timeframe
    df = df.filter(
        (pl.col("first_pixel_time") >= pl.lit(start)) &
        (pl.col("first_pixel_time") <= pl.lit(end))
    )
    # Only get 1 row for each user_id
    df = df.unique(subset="user_id")
    df = df.select(
        pl.col("user_id").len().alias("count")
    )
    return df.item(0, "count")

def main():
    # Start Timer
    time_start = perf_counter_ns()
    # Convert Start and End to Datetime
    start = datetime.strptime(sys.argv[1], '%Y-%m-%d %H')
    end = datetime.strptime(sys.argv[2], '%Y-%m-%d %H')
    if start > end:
        print("Start date is after end date")
        sys.exit(1)
    # Read Parquet into Lazy Frame
    df = pl.scan_parquet("new_dataset.parquet").filter(
        (pl.col("timestamp") >= start) & (pl.col("timestamp") <= end)
    ).collect()
    # Color Ranking
    print("Color ranking:",
        color_ranking(df.select(["user_id", "pixel_color"])))
    # Average Session Length
    print("Avg session length in H:M:S.MS :", avg_session_length(
        df.select(["user_id", "timestamp"])))
    # Pixel Placement
    percentile_arr = pixel_placement(df.select(["user_id"]))
    print("x% of users placed less than __ pixels in the timeframe"
        "\n50th Percentile:", percentile_arr[0],
        "\n75th Percentile:", percentile_arr[1],
        "\n90th Percentile:", percentile_arr[2],
        "\n99th Percentile:", percentile_arr[3]
    )
    # First time Users
    # needs the whole dataset
    other_df = pl.scan_parquet("new_dataset.parquet").select(
        ["timestamp", "user_id"]).collect()
    print("Count first time users",
          first_time_users(other_df, start, end))
    # Stop Timer
    stop = perf_counter_ns()
    print("Time:", (stop-time_start) / 1000000, 'ms')

    return 0

if __name__ == '__main__':
    # Make sure 3 args are entered
    if len(sys.argv) != 3:
        print("3 args required: file start end")
        sys.exit(1)
    main()
'''
Questions: How do I approach percentile? Do i group by color count first and sort?
- Color placing: how do I figure out color names? 
- YES Does the same user placing a black and red pixel for both count?
- Mapping ids hint
'''
'''
Notes:
- Parquet file is in datetime format DONE
for all users placed # of pixels placed sort by # pixels placed
'''
# Lag(1)
# Datediff
# Segment to calculate sessions
# Remove sessions that are only length 1
# For each session, calculate min and max
