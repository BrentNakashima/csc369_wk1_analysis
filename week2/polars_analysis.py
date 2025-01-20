import sys
import polars as pl
from datetime import datetime
from time import perf_counter_ns
def mostPlacedColor(color_df):
    # Goal: Return the most placed color
    # Count occurrences sort by DESC
    # Return the first row (the max)
    #print(color_df.group_by("pixel_color").len().sort("len", descending=True))
    return color_df.group_by("pixel_color").len().sort("len", descending = True)["pixel_color"][0]
    
def mostPlacedPixel(coord_df):
    # Goal: Return the most placed coord
    # Count occurrences sort by DESC
    # Return the first row (the max)
    return coord_df.group_by("coordinate").len().sort("len", descending = True)["coordinate"][0]
def main():
    # Start Timer
    time_start = perf_counter_ns()
    #print("Test:", start[5:], end[5:])
    start = datetime.strptime(sys.argv[1], '%Y-%m-%d %H')
    end = datetime.strptime(sys.argv[2], '%Y-%m-%d %H')
    #print(start2, end2)
    # Validate End Date is After Start Date
    if start > end:
        print("Start date is after end date")
        sys.exit(1)
    
    # Note: Could try lazy
    # Read file using projection (SQL Select)
    color_df = pl.read_csv("new_dataset.csv", columns=["timestamp", "pixel_color"], try_parse_dates=True)
    coord_df = pl.read_csv("new_dataset.csv", columns=["timestamp", "coordinate"], try_parse_dates=True)
    
    # Filter df using predicate (SQL Where)
    color_filter_df = color_df.filter(pl.col("timestamp").is_between(start, end))
    coord_filter_df = coord_df.filter(pl.col("timestamp").is_between(start, end))
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
