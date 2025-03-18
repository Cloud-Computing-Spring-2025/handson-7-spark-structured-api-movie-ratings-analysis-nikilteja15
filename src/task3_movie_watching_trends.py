from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def initialize_spark(app_name="Task3_Trend_Analysis"):
    return SparkSession.builder.appName(app_name).getOrCreate()

def load_data(spark, file_path):
    schema = """
        UserID INT, MovieID INT, MovieTitle STRING, Genre STRING, Rating FLOAT, ReviewCount INT,
        WatchedYear INT, UserLocation STRING, AgeGroup STRING, StreamingPlatform STRING,
        WatchTime INT, IsBingeWatched BOOLEAN, SubscriptionStatus STRING
    """
    return spark.read.csv(file_path, header=True, schema=schema)

def analyze_movie_watching_trends(df):
    trends_df = df.groupBy("WatchedYear").agg(count("*").alias("MoviesWatched")) \
                  .orderBy("WatchedYear")
    return trends_df

def write_output(result_df, output_path):
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def main():
    spark = initialize_spark()
    input_file = "input/movie_ratings_data.csv"
    output_file = "outputs/movie_watching_trends.csv"
    
    df = load_data(spark, input_file)
    result_df = analyze_movie_watching_trends(df)
    write_output(result_df, output_file)
    spark.stop()

if __name__ == "__main__":
    main()
