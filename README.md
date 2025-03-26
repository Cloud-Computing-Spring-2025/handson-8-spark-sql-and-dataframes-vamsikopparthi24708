# handson-08-sparkSQL-dataframes-social-media-sentiment-analysis

## *Overview*

In this assignment, you will leverage Spark Structured APIs to analyze a dataset containing employee information from various departments within an organization. Your goal is to extract meaningful insights related to employee satisfaction, engagement, concerns, and job titles. This exercise is designed to enhance your data manipulation and analytical skills using Spark's powerful APIs.

## *Objectives*

By the end of this assignment, you should be able to:

1. *Data Loading and Preparation*: Import and preprocess data using Spark Structured APIs.
2. *Data Analysis*: Perform complex queries and transformations to address specific business questions.
3. *Insight Generation*: Derive actionable insights from the analyzed data.

## *Dataset*

## **Dataset: posts.csv **

You will work with a dataset containing information about *100+ users* who rated movies across various streaming platforms. The dataset includes the following columns:

| Column Name     | Type    | Description                                           |
|-----------------|---------|-------------------------------------------------------|
| PostID          | Integer | Unique ID for the post                                |
| UserID          | Integer | ID of the user who posted                             |
| Content         | String  | Text content of the post                              |
| Timestamp       | String  | Date and time the post was made                       |
| Likes           | Integer | Number of likes on the post                           |
| Retweets        | Integer | Number of shares/retweets                             |
| Hashtags        | String  | Comma-separated hashtags used in the post             |
| SentimentScore  | Float   | Sentiment score (-1 to 1, where -1 is most negative)  |


---

## **Dataset: users.csv **
| Column Name | Type    | Description                          |
|-------------|---------|--------------------------------------|
| UserID      | Integer | Unique user ID                       |
| Username    | String  | User's handle                        |
| AgeGroup    | String  | Age category (Teen, Adult, Senior)   |
| Country     | String  | Country of residence                 |
| Verified    | Boolean | Whether the account is verified      |

---

### *Sample Data*

Below is a snippet of the posts.csv,users.csv to illustrate the data structure. Ensure your dataset contains at least 100 records for meaningful analysis.


PostID,UserID,Content,Timestamp,Likes,Retweets,Hashtags,SentimentScore
101,1,"Loving the new update! #tech #innovation","2023-10-05 14:20:00",120,45,"#tech,#innovation",0.8
102,2,"This app keeps crashing. Frustrating! #fail","2023-10-05 15:00:00",5,1,"#fail",-0.7
103,3,"Just another day... #mood","2023-10-05 16:30:00",15,3,"#mood",0.0
104,4,"Absolutely love the UX! #design #cleanUI","2023-10-06 09:10:00",75,20,"#design,#cleanUI",0.6
105,5,"Worst experience ever. Fix it. #bug","2023-10-06 10:45:00",2,0,"#bug",-0.9


---


UserID,Username,AgeGroup,Country,Verified
1,@techie42,Adult,US,True
2,@critic99,Senior,UK,False
3,@daily_vibes,Teen,India,False
4,@designer_dan,Adult,Canada,True
5,@rage_user,Adult,US,False


---



## *Assignment Tasks*

You are required to complete the following three analysis tasks using Spark Structured APIs. Ensure that your analysis is well-documented, with clear explanations and any relevant visualizations or summaries.

### **1. Hashtag Trends **

*Objective:*

Identify trending hashtags by analyzing their frequency of use across all posts.

*Tasks:*

- *Extract Hashtags*: Split the Hashtags column and flatten it into individual hashtag entries.
- *Count Frequency*: Count how often each hashtag appears.
- *Find Top Hashtags*: Identify the top 10 most frequently used hashtags.


*Expected Outcome:*  
A ranked list of the most-used hashtags and their frequencies.


### *2. Engagement by Age Group*

*Objective:*  
Understand how users from different age groups engage with content based on likes and retweets.

*Tasks:*

- *Join Datasets*: Combine posts.csv and users.csv using UserID.
- *Group by AgeGroup*: Calculate average likes and retweets for each age group.
- *Rank Groups*: Sort the results to highlight the most engaged age group.

*Expected Outcome:*  
A summary of user engagement behavior categorized by age group.

---

### *3. Sentiment vs Engagement*

*Objective:*  
Evaluate how sentiment (positive, neutral, or negative) influences post engagement.

*Tasks:*

- *Categorize Posts*: Group posts into Positive (>0.3), Neutral (-0.3 to 0.3), and Negative (< -0.3) sentiment groups.
- *Analyze Engagement*: Calculate average likes and retweets per sentiment category.

*Expected Outcome:*  
Insights into whether happier or angrier posts get more attention.



---

### *4. Top Verified Users by Reach*

*Objective:*  
Find the most influential verified users based on their post reach (likes + retweets).

*Tasks:*

- *Filter Verified Users*: Use Verified = True from users.csv.
- *Calculate Reach*: Sum likes and retweets for each user.
- *Rank Users*: Return top 5 verified users with highest total reach.

*Expected Outcome:*  
A leaderboard of verified users based on audience engagement.

---

## *Prerequisites*

Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

1. *Python 3.x*:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     bash
     python3 --version
     

2. *PySpark*:
   - Install using pip:
     bash
     pip install pyspark
     

3. *Apache Spark*:
   - Ensure Spark is installed. You can download it from the [Apache Spark Downloads](https://spark.apache.org/downloads.html) page.
   - Verify installation by running:
     bash
     spark-submit --version
     


## *Setup Instructions*

### *1. Project Structure*

Ensure your project directory follows the structure below:


```
├── input/
│   ├── posts.csv
│   └── users.csv
├── outputs/
│   ├── hashtag_trends.csv
│   ├── engagement_by_age.csv
│   ├── sentiment_engagement.csv
│   └── top_verified_users.csv
├── src/
│   ├── task1_hashtag_trends.py
│   ├── task2_engagement_by_age.py
│   ├── task3_sentiment_vs_engagement.py
│   └── task4_top_verified_users.py
├── docker-compose.yml
└── README.md
```



- *input/*: Contains the input datasets (posts.csv and users.csv)  
- *outputs/*: Directory where the results of each task will be saved.
- *src/*: Contains the individual Python scripts for each task.
- *docker-compose.yml*: Docker Compose configuration file to set up Spark.
- *README.md*: Assignment instructions and guidelines.

### *2. Running the Analysis Tasks*

You can run the analysis tasks either locally or using Docker.

#### *a. Running Locally*

1. **Execute Each Task Using spark-submit**:
   ```bash
 
     spark-submit src/task1_hashtag_trends.py
     spark-submit src/task2_engagement_by_age.py
     spark-submit src/task3_sentiment_vs_engagement.py
     spark-submit src/task4_top_verified_users.py
     ```
   

2. *Verify the Outputs*:
   Check the outputs/ directory for the resulting files:
   ```bash
   ls outputs/
   ```

## Codes and outputs
### Task 1 code

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv")

# TODO: Split the Hashtags column into individual hashtags and count the frequency of each hashtag and sort descending
# Split the Hashtags column into an array, then explode it into individual rows
hashtags_df = posts_df.select(explode(split(col("Hashtags"), ",")).alias("Hashtag"))

# Count the frequency of each hashtag and sort descending
hashtag_counts = hashtags_df.groupBy("Hashtag").count().orderBy(col("count").desc())


# Save result
hashtag_counts.coalesce(1).write.mode("overwrite").csv("outputs/hashtag_trends.csv", header=True)
```


### Task 1 output
[Task 1 outpout](./outputs/hashtag_trends.csv/part-00000-9efb3d40-df87-4932-88b2-91a99a04a7d2-c000.csv)

### Task 2 code

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

spark = SparkSession.builder.appName("EngagementByAgeGroup").getOrCreate()

# Load datasets
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)
users_df = spark.read.option("header", True).csv("input/users.csv", inferSchema=True)

# Join on UserID
joined_df = posts_df.join(users_df, on="UserID")

# Group by AgeGroup and calculate average Likes and Retweets
engagement_df = joined_df.groupBy("AgeGroup").agg(
    avg("Likes").alias("Avg_Likes"),
    avg("Retweets").alias("Avg_Retweets")
).orderBy(cola("Avg_Likes").desc())

# Save result
engagement_df.coalesce(1).write.mode("overwrite").csv("outputs/engagement_by_age.csv", header=True)
```


### Task 2 output
[Task 2 outpout](./outputs/engagement_by_age.csv/part-00000-f2a7d089-a0f9-4558-b358-adca33c68f04-c000.csv)


### Task 3 code

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, avg, col

spark = SparkSession.builder.appName("SentimentVsEngagement").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)

# Categorize sentiment
posts_with_sentiment = posts_df.withColumn(
    "Sentiment",
    when(col("SentimentScore") > 0.3, "Positive")
    .when(col("SentimentScore") < -0.3, "Negative")
    .otherwise("Neutral")
)

# Group by sentiment and calculate average Likes and Retweets
sentiment_stats = posts_with_sentiment.groupBy("Sentiment").agg(
    avg("Likes").alias("Avg_Likes"),
    avg("Retweets").alias("Avg_Retweets")
).orderBy(col("Avg_Likes").desc())

# Save result
sentiment_stats.coalesce(1).write.mode("overwrite").csv("outputs/sentiment_engagement.csv", header=True)
```

### Task 3 output
[Task 3 outpout](./outputs/sentiment_engagement.csv/part-00000-36b017c9-f5be-498c-9e5c-ca79299e1b0d-c000.csv)

### Task 4 code

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

spark = SparkSession.builder.appName("TopVerifiedUsers").getOrCreate()

# Load datasets
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)
users_df = spark.read.option("header", True).csv("input/users.csv", inferSchema=True)

# Filter for verified users
verified_users_df = users_df.filter(col("Verified") == True)

# Join with posts
verified_posts_df = posts_df.join(verified_users_df, on="UserID")

# Calculate total reach per user
reach_df = verified_posts_df.groupBy("Username").agg(
    _sum(col("Likes") + col("Retweets")).alias("Total_Reach")
)

# Get top 5 verified users by reach
top_verified = reach_df.orderBy(col("Total_Reach").desc()).limit(5)

# Save result
top_verified.coalesce(1).write.mode("overwrite").csv("outputs/top_verified_users.csv", header=True)
```


### Task 4 output
[Task 4 outpout](./outputs/top_verified_users.csv/part-00000-1d1d6f8c-7383-4ae3-a99d-76733e85f515-c000.csv)
