from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def initialize_spark(app_name="Task2_Churn_Risk_Users"):
    return SparkSession.builder.appName(app_name).getOrCreate()

def load_data(spark, file_path):
    schema = """
        UserID INT, MovieID INT, MovieTitle STRING, Genre STRING, Rating FLOAT, ReviewCount INT,
        WatchedYear INT, UserLocation STRING, AgeGroup STRING, StreamingPlatform STRING,
        WatchTime INT, IsBingeWatched BOOLEAN, SubscriptionStatus STRING
    """
    return spark.read.csv(file_path, header=True, schema=schema)

def identify_churn_risk_users(df):
    """
    Identify users with canceled subscriptions and low watch time (<100 minutes).
    """
    churn_risk_df = df.filter((col("SubscriptionStatus") == "Canceled") & (col("WatchTime") < 100))
    churn_count = churn_risk_df.count()

    # Create a DataFrame matching the expected format
    result = [( "Users with low watch time & canceled subscriptions", churn_count )]
    columns = ["Churn Risk Users", "Total Users"]

    return df.sparkSession.createDataFrame(result, columns)


def write_output(result_df, output_path):
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def main():
    spark = initialize_spark()
    input_file = "input/movie_ratings_data.csv"
    output_file = "outputs/churn_risk_users.csv"
    
    df = load_data(spark, input_file)
    result_df = identify_churn_risk_users(df)
    write_output(result_df, output_file)
    spark.stop()

if __name__ == "__main__":
    main()
