# Movie Ratings Analysis

## **Prerequisites**

Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

1. **Python 3.x**:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. **PySpark**:
   - Install using `pip`:
     ```bash
     pip install pyspark
     ```

3. **Apache Spark**:
   - Ensure Spark is installed. You can download it from the [Apache Spark Downloads](https://spark.apache.org/downloads.html) page.
   - Verify installation by running:
     ```bash
     spark-submit --version
     ```

4. **Docker & Docker Compose** (Optional):
   - If you prefer using Docker for setting up Spark, ensure Docker and Docker Compose are installed.
   - [Docker Installation Guide](https://docs.docker.com/get-docker/)
   - [Docker Compose Installation Guide](https://docs.docker.com/compose/install/)

## **Setup Instructions**

### **1. Project Structure**

Ensure your project directory follows the structure below:

```
MovieRatingsAnalysis/
├── input/
│   └── movie_ratings_data.csv
├── outputs/
│   ├── binge_watching_patterns.csv
│   ├──churn_risk_users.csv
│   └── movie_watching_trends.csv
├── src/
│   ├── task1_binge_watching_patterns.py
│   ├── task2_churn_risk_users.py
│   └── task3_movie_watching_trends.py
├── docker-compose.yml
└── README.md
```









- **input/**: Contains the `movie_ratings_data.csv` dataset.
- **outputs/**: Directory where the results of each task will be saved.
- **src/**: Contains the individual Python scripts for each task.
- **docker-compose.yml**: Docker Compose configuration file to set up Spark.
- **README.md**: Assignment instructions and guidelines.

### **2. Running the Analysis Tasks**

You can run the analysis tasks either locally or using Docker.

#### **a. Running Locally**

1. **Navigate to the Project Directory**:
   ```bash
   cd MovieRatingsAnalysis/
   ```

2. **Execute Each Task Using `spark-submit`**:
   ```bash
   spark-submit src/task1_binge_watching_patterns.py
   spark-submit src/task2_churn_risk_users.py
   spark-submit src/task3_movie_watching_trends.py
   ```

3. **Verify the Outputs**:
   Check the `outputs/` directory for the resulting files:
   ```bash
   ls outputs/
   ```
   You should see:
   - `binge_watching_patterns.txt`
   - `churn_risk_users.csv`
   - `movie_watching_trends.csv`

#### **b. Running with Docker (Optional)**

1. **Start the Spark Cluster**:
   ```bash
   docker-compose up -d
   ```

2. **Access the Spark Master Container**:
   ```bash
   docker exec -it spark-master bash
   ```

3. **Navigate to the Spark Directory**:
   ```bash
   cd /opt/bitnami/spark/
   ```

4. **Run Your PySpark Scripts Using `spark-submit`**:
   ```bash
   spark-submit src/task1_binge_watching_patterns.py
   spark-submit src/task2_churn_risk_users.py
   spark-submit src/task3_movie_watching_trends.py
   ```

5. **Exit the Container**:
   ```bash
   exit
   ```

6. **Verify the Outputs**:
   On your host machine, check the `outputs/` directory for the resulting files.

7. **Stop the Spark Cluster**:
   ```bash
   docker-compose down
   ```

## **Overview**

In this assignment, you will leverage Spark Structured APIs to analyze a dataset containing employee information from various departments within an organization. Your goal is to extract meaningful insights related to employee satisfaction, engagement, concerns, and job titles. This exercise is designed to enhance your data manipulation and analytical skills using Spark's powerful APIs.

## **Objectives**

By the end of this assignment, you should be able to:

1. **Data Loading and Preparation**: Import and preprocess data using Spark Structured APIs.
2. **Data Analysis**: Perform complex queries and transformations to address specific business questions.
3. **Insight Generation**: Derive actionable insights from the analyzed data.

## **Dataset**

## **Dataset: Advanced Movie Ratings & Streaming Trends**

You will work with a dataset containing information about **100+ users** who rated movies across various streaming platforms. The dataset includes the following columns:

| **Column Name**         | **Data Type**  | **Description** |
|-------------------------|---------------|----------------|
| **UserID**             | Integer       | Unique identifier for a user |
| **MovieID**            | Integer       | Unique identifier for a movie |
| **MovieTitle**         | String        | Name of the movie |
| **Genre**             | String        | Movie genre (e.g., Action, Comedy, Drama) |
| **Rating**            | Float         | User rating (1.0 to 5.0) |
| **ReviewCount**       | Integer       | Total reviews given by the user |
| **WatchedYear**       | Integer       | Year when the movie was watched |
| **UserLocation**      | String        | User's country |
| **AgeGroup**          | String        | Age category (Teen, Adult, Senior) |
| **StreamingPlatform** | String        | Platform where the movie was watched |
| **WatchTime**        | Integer       | Total watch time in minutes |
| **IsBingeWatched**    | Boolean       | True if the user watched 3+ movies in a day |
| **SubscriptionStatus** | String        | Subscription status (Active, Canceled) |

---



### **Sample Data**

Below is a snippet of the `movie_ratings_data.csv` to illustrate the data structure. Ensure your dataset contains at least 100 records for meaningful analysis.

```
UserID,MovieID,MovieTitle,Genre,Rating,ReviewCount,WatchedYear,UserLocation,AgeGroup,StreamingPlatform,WatchTime,IsBingeWatched,SubscriptionStatus
1,101,Inception,Sci-Fi,4.8,12,2022,US,Adult,Netflix,145,True,Active
2,102,Titanic,Romance,4.7,8,2021,UK,Adult,Amazon,195,False,Canceled
3,103,Avengers: Endgame,Action,4.5,15,2023,India,Teen,Disney+,180,True,Active
4,104,The Godfather,Crime,4.9,20,2020,US,Senior,Amazon,175,False,Active
5,105,Forrest Gump,Drama,4.8,10,2022,Canada,Adult,Netflix,130,True,Active
...
```

## **Assignment Tasks**

You are required to complete the following three analysis tasks using Spark Structured APIs. Ensure that your analysis is well-documented, with clear explanations and any relevant visualizations or summaries.

### **1. Identify Departments with High Satisfaction and Engagement**

**Objective:**

Determine which movies have an average watch time greater than 100 minutes and rank them based on user engagement.

**Tasks:**

- **Filter Movies**: Select movies that have been watched for more than 100 minutes on average.
- **Analyze Average Watch Time**: Compute the average watch time per user for each movie.
- **Identify Top Movies**: List movies where the average watch time is among the highest.

#### **Code - src/task1_binge_watching_patterns.py**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round as spark_round, concat, lit

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
    # Filter binge-watched records
    binge_df = df.filter(col("IsBingeWatched") == True)

    # Count binge watchers per age group
    binge_counts = binge_df.groupBy("AgeGroup").agg(count("*").alias("BingeWatchers"))

    # Total users per age group
    total_counts = df.groupBy("AgeGroup").agg(count("*").alias("TotalUsers"))

    # Join and calculate percentage with % symbol
    result_df = binge_counts.join(total_counts, "AgeGroup") \
        .filter(col("TotalUsers") > 0) \
        .withColumn("PercentageValue", spark_round((col("BingeWatchers") / col("TotalUsers")) * 100, 2)) \
        .withColumn("Percentage", concat(col("PercentageValue").cast("string"), lit("%"))) \
        .select("AgeGroup", "BingeWatchers", "Percentage") \
        .orderBy("AgeGroup")

    return result_df

def write_output(result_df, output_path):
    # Save as CSV
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def main():
    spark = initialize_spark()
    input_file = "input/movie_ratings_data.csv"
    output_file = "outputs/binge_watching_patterns.csv"

    df = load_data(spark, input_file)
    result_df = detect_binge_watching_patterns(df)
    result_df.show()  # Optional: Preview output in console
    write_output(result_df, output_file)
    spark.stop()

if __name__ == "__main__":
    main()


```

**Expected Outcome:**

A list of departments meeting the specified criteria, along with the corresponding percentages.

**Example Output:**

| Age Group   | Binge Watchers | Percentage |
|-------------|----------------|------------|
| Adult       | 14             | 43.75%     |
| Senior      | 13             | 39.39%     |
| Teen        | 23             | 65.71%     |



---

### **2. Identify Churn Risk Users**  

**Objective:**  

Find users who are **at risk of churn** by identifying those with **canceled subscriptions and low watch time (<100 minutes)**.

**Tasks:**  

- **Filter Users**: Select users who have `SubscriptionStatus = 'Canceled'`.  
- **Analyze Watch Time**: Identify users with `WatchTime < 100` minutes.  
- **Count At-Risk Users**: Compute the total number of such users.

  #### **Code - src/task2_churn_risk_users.py**
```python
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

```

**Expected Outcome:**  

A count of users who **canceled their subscriptions and had low engagement**, highlighting **potential churn risks**.

**Example Output:**  


|Churn Risk Users                                  |	Total Users |
|--------------------------------------------------|--------------|
|Users with low watch time & canceled subscriptions|	10          |



---

### **3. Trend Analysis Over the Years**  

**Objective:**  

Analyze how **movie-watching trends** have changed over the years and find peak years for movie consumption.

**Tasks:**  

- **Group by Watched Year**: Count the number of movies watched in each year.  
- **Analyze Trends**: Identify patterns and compare year-over-year growth in movie consumption.  
- **Find Peak Years**: Highlight the years with the highest number of movies watched.

  #### **Code - src/task3_movie_watching_trends.py**
```python
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

```

**Expected Outcome:**  

A summary of **movie-watching trends** over the years, indicating peak years for streaming activity.

**Example Output:**  

| Watched Year | Movies watched |
|--------------|----------------|
| 2018         | 19             |
| 2019         | 18             |
| 2020         | 12             |
| 2021         | 15             |
| 2022         | 15             |
| 2023         | 21             |  


---


## **Challenges Faced**
- Handling large CSV datasets efficiently in Spark.
- Joining DataFrames accurately while maintaining data integrity.
- Computing percentages and rounding results correctly.
- Filtering data with multiple conditions using PySpark APIs.

## **Approach**
- Loaded CSV data into Spark DataFrames with predefined schema.
- Used DataFrame filtering and grouping operations to analyze each task.
- Applied Spark SQL functions like `count`, `round`, and `join` for calculations.
- Structured the output in the required format and wrote to CSV.

## **Findings**
- **Teenagers** binge-watch the most compared to other age groups.
- **Churn Risk Users** with less than 100 minutes watch time and canceled subscriptions were identified clearly.
- Movie-watching peaked in **2023** based on the dataset provided.

👍 **Good luck, and happy analyzing!**




