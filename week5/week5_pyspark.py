from pyspark.sql import SparkSession
import os

def main():
    # Create Spark Session
    spark = SparkSession.builder.appName(
        "Parquet File").getOrCreate()
    # Load into df
    df = spark.read.parquet("new_dataset.parquet").select("user_id", "coordinate")
    # df = spark.read.parquet("sample.parquet")
    # Only select user_id, coordinate
    #df2 = df.select("user_id", "coordinate")
    #df2 = df2.filter
    df.show()
    # Create temporary view
    df.createOrReplaceTempView("temp_view")
    # SQL Query to get # of pixels placed for each user and each coord
    '''
    result_df = spark.sql("""
        SELECT user_id, coordinate, COUNT(*) AS pixels_placed
        FROM temp_view
        GROUP BY coordinate, user_id
        ORDER BY user_id, pixels_placed DESC
        """)
    '''
    print("Temp view created")
    # SQL Query to Only Get Highest Pixel Placed Coord for Each User
    # This works!
    result_df = spark.sql("""
        SELECT user_id, coordinate, pixels_placed
        FROM (
            SELECT user_id, coordinate, COUNT(*) AS pixels_placed,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY COUNT(*) DESC) AS rn
            FROM temp_view
            GROUP BY coordinate, user_id
        ) AS table
        WHERE rn = 1
        ORDER BY user_id, pixels_placed DESC;
    """)
    result_df.show()
    
    # Stop Spark Session
    spark.stop()                                
    return 0


if __name__ == '__main__':
    main()
