import sys
import duckdb
from datetime import datetime
import polars as pl
from time import perf_counter_ns
    
def main():
    # Start Timer
    time_start = perf_counter_ns()
    # Connect to in memory storage
    conn = duckdb.connect(database=':memory')
    # load parquet into duck db table
    '''conn.execute("""CREATE TABLE t 
                 AS SELECT user_id, coordinate
                FROM 'new_dataset.parquet'
                """)'''
    # Query to Calculate Most Painted Pixel by each user and how many times
    '''query = """
        SELECT user_id, coordinate, pixels_placed
        FROM (
            SELECT user_id, coordinate, COUNT(*) AS pixels_placed,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY COUNT(*) DESC) AS rn
            FROM "new_dataset.parquet"
            GROUP BY coordinate, user_id
        ) AS t
        WHERE rn = 1
        ORDER BY pixels_placed DESC
        LIMIT 20;
        """
    '''
    query2 = """
        SELECT AVG(pixels_placed)
        FROM (
            SELECT user_id, coordinate, pixels_placed
            FROM (
                SELECT user_id, coordinate, COUNT(*) AS pixels_placed,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY COUNT(*) DESC) AS rn
                FROM "new_dataset.parquet"
                GROUP BY coordinate, user_id
            ) AS t
            WHERE rn = 1
            ORDER BY pixels_placed DESC
        ) AS t2;
        """
    # result = conn.execute(query).df()
    result = conn.execute(query2).df()
    print(result)
    # Stop timer
    stop = perf_counter_ns()
    print("Time:", (stop-time_start) / 1000000, 'ms')
    return

if __name__ == '__main__':
    main()
