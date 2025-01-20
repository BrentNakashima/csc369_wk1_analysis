import sys
import duckdb
from datetime import datetime
import polars as pl
from time import perf_counter_ns
    
def main():
    # Start Timer
    time_start = perf_counter_ns()
    start = datetime.strptime(sys.argv[1], '%Y-%m-%d %H')
    end = datetime.strptime(sys.argv[2], '%Y-%m-%d %H')
    # Validate End Date is After Start Date
    if start > end:
        print("Start date is after end date")
        sys.exit(1)
    conn = duckdb.connect(database=':memory')
    
    # have to use in memory storage
    # Create SQL Queries (limit 1 order by desc to just get the max)
    color_query = """
        SELECT pixel_color, COUNT(pixel_color) AS occurrences 
        FROM "new_dataset.csv"
        WHERE timestamp BETWEEN ? AND ?
        GROUP BY pixel_color 
        ORDER BY occurrences DESC
        LIMIT 1;
        """
    color_result = conn.execute(color_query, [start,end]).fetchall()
    print("Most placed pixel color:", color_result)
    
    coord_query = """
        SELECT coordinate, COUNT(coordinate) AS occurrences 
        FROM "new_dataset.csv"
        WHERE timestamp BETWEEN ? AND ?
        GROUP BY coordinate 
        ORDER BY occurrences DESC
        LIMIT 1;
        """
    coord_result = conn.execute(coord_query, [start,end]).fetchall()
    print("Most placed pixel location:", coord_result)
   
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
