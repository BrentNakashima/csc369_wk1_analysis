import sys
import pandas as pd
from datetime import datetime
from time import perf_counter_ns
def mostPlacedColor(color_df):
    # Goal: Return the most placed color
    # Count occurrences sort by DESC
    # Return the first row (the max)
    df = color_df.groupby(["pixel_color"]).size().reset_index(name="count").sort_values(by="count", ascending=False)
    return df.iloc[0]["pixel_color"]
    
def mostPlacedPixel(coord_df):
    # Goal: Return the most placed coord
    # Count occurrences sort by DESC
    # Return the first row (the max)
    df = coord_df.groupby(["coordinate"]).size().reset_index(name="count").sort_values(by="count", ascending=False)
    return df.iloc[0]["coordinate"]
def main():
    # Start Timer
    time_start = perf_counter_ns()
    start = datetime.strptime(sys.argv[1], '%Y-%m-%d %H')
    end = datetime.strptime(sys.argv[2], '%Y-%m-%d %H')
    # Validate End Date is After Start Date
    if start > end:
        print("Start date is after end date")
        sys.exit(1)
    
    # Note: Could try lazy
    # Read file using projection (SQL Select)
    color_df = pd.read_csv("new_dataset.csv", usecols=["timestamp", "pixel_color"], parse_dates=['timestamp'])
    coord_df = pd.read_csv("new_dataset.csv", usecols=["timestamp", "coordinate"], parse_dates=['timestamp'])
    # Filter df using predicate (SQL Where)
    color_filter_df = color_df[(color_df["timestamp"] >= start) & (color_df["timestamp"] <= end)]
    coord_filter_df = coord_df[(coord_df["timestamp"] >= start) & (coord_df["timestamp"] <= end)]
    
    # Call functions
    print("Most placed pixel color:", mostPlacedColor(color_filter_df))
    print("Most placed pixel location:", mostPlacedPixel(coord_filter_df))
    # Stop timer
    stop = perf_counter_ns()
    print("Time:", (stop-time_start) / 1000000, 'ms')
    return
if __name__ == '__main__':
    # Make sure a start and end time are entered in CLI
    if len(sys.argv) != 3:
        print("Not enough args entered")
        print("Args: file, start date, end date")
        sys.exit(1) # Not normal exit
    main()
