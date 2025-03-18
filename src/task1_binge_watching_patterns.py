from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round as spark_round

def initialize_spark(app_name="Task1_Binge_Watching_Patterns"):
    return SparkSession.builder.appName(app_name).getOrCreate()

def load_data(spark, file_path):
    schema = """
        UserID INT, MovieID INT, MovieTitle STRING, Genre STRING, Rating FLOAT, ReviewCount INT,
        WatchedYear INT, UserLocation STRING, AgeGroup STRING, StreamingPlatform STRING,
        WatchTime INT, IsBingeWatched BOOLEAN, SubscriptionStatus STRING
    """
    return spark.read.csv(file_path, header=True, schema=schema)

def detect_binge_watching_patterns(df):
    binge_df = df.filter(col("IsBingeWatched") == True)
    binge_counts = binge_df.groupBy("AgeGroup").agg(count("*").alias("BingeWatchers"))
    total_counts = df.groupBy("AgeGroup").agg(count("*").alias("TotalUsers"))
    
    result_df = binge_counts.join(total_counts, "AgeGroup") \
        .withColumn("Percentage", spark_round((col("BingeWatchers") / col("TotalUsers")) * 100, 2)) \
        .select("AgeGroup", "BingeWatchers", "Percentage") \
        .orderBy("AgeGroup")
    
    return result_df

def write_output(result_df, output_path):
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def main():
    spark = initialize_spark()
    input_file = "input/movie_ratings_data.csv"
    output_file = "outputs/binge_watching_patterns.csv"
    
    df = load_data(spark, input_file)
    result_df = detect_binge_watching_patterns(df)
    write_output(result_df, output_file)
    spark.stop()

if __name__ == "__main__":
    main()
