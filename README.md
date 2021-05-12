## New York Cabs Travel Behavior Data Pipeline
## Presentation
We want to study the travel behavior of New York cabs. To do this we calculate the following indicators:
- the average speed of each trip,
- the number of trips made according to the day of the week,
- the number of trips made according to the time of day per 4-hour period,
- the number of km traveled per day of the week.

## Data
The data and their descriptions are on the link below:
https://www.kaggle.com/c/nyc-taxi-trip-duration/data \
For this study it is necessary to download only the file: train.csv (200 Mo)
- train.csv - the training set (contains 1458644 trip records)

## Data fields
- ``id``: a unique identifier for each trip
- ``vendor_id``: a code indicating the provider associated with the trip record
- ``pickup_datetime``: date and time when the meter was engaged
- ``dropoff_datetime``: date and time when the meter was disengaged
- ``passenger_count``: the number of passengers in the vehicle (driver entered value)
- ``pickup_longitude``: the longitude where the meter was engaged
- ``pickup_latitude``: the latitude where the meter was engaged
- ``dropoff_longitude``: the longitude where the meter was disengaged
- ``dropoff_latitude``: the latitude where the meter was disengaged
- ``store_and_fwd_flag``: This flag indicates whether the trip record was held in vehicle memory before sending to the vendor because the vehicle did not have a connection to the server - ``Y=store`` and forward; ``N=not`` a store and forward trip
- ``trip_duration``: duration of the trip in seconds

## Step 1
### Connecting Drive to Colab
The first thing you want to do when you are working on Colab is mounting your Google Drive. This will enable you to access any directory on your Drive inside the Colab notebook
````
from google.colab import drive
drive.mount('/content/drive')
````
Once you have done that, the next obvious step is to load the data.

## Step 2
### Reading Data from Drive
Since we are working on a large file, the best way to upload data to Drive is in a zip format. Just drag and drop your zip folder inside any directory you want on Drive.

We need to provide the path to the zip folder along with the ``!unzip`` command.
````
!unzip "/content/drive/My Drive/AV articles/PySpark on Colab/black_friday_train.zip"
````

## Step 3
### Setting up PySpark in Colab
Spark is written in the Scala programming language and requires the Java Virtual Machine (JVM) to run. Therefore, our first task is to download Java.
````
!apt-get install openjdk-8-jdk-headless -qq > /dev/null
````
Next, we will install Apache Spark 3.1.1 with Hadoop 2.7.
````
!wget -q https://downloads.apache.org/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz
````
Now, we just need to unzip that folder.
````
!tar xf spark-3.0.1-bin-hadoop2.7.tgz
````
To install findspark library \
It will locate Spark on the system and import it as a regular library.
````
!pip install -q findspark
````
Now that we have installed all the necessary dependencies in Colab, it is time to set the environment path. This will enable us to run Pyspark in the Colab environment.
````
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.0.1-bin-hadoop2.7"
````
We need to locate Spark in the system. For that, we import findspark and use the findspark.init() method.
````
import findspark
findspark.init()
````
If you want to know the location where Spark is installed, use:
````
findspark.find()
````
To view the Spark UI, created few more lines of code to create a public URL for the UI page.
````
!wget https://bin.equinox.io/c/4VmDzA7iaHb/ngrok-stable-linux-amd64.zip
!unzip ngrok-stable-linux-amd64.zip
get_ipython().system_raw('./ngrok http 4050 &')
!curl -s http://localhost:4040/api/tunnels
````
We would be able to view the jobs and their stages at the link created

## Step 4
### Loading data into PySpark
First thing first, we need to load the dataset. We will use the read.csv module. The inferSchema parameter provided will enable Spark to automatically determine the data type for each column but it has to go over the data once. If you don’t want that to happen, then you can instead provide the schema explicitly in the schema parameter.
````
data = spark.read.csv("train.csv", header=True, inferSchema=True)
````
## Step 5
### Show column details
The first step in an exploratory data analysis is to check out the schema of the dataframe. This will give you a bird’s-eye view of the columns in the dataframe along with their data types.

````
data.printSchema()
````
## Step 6
### Display Rows
You can provide the number of rows you want to print within the parenthesis.
````
data.show(5)
````
## Step 7
### Number of rows in DF
to know the total number of rows in the dataframe
````
df.count()
````
## Step 8
### Display specific columns
To view some specific columns from the dataframe
````
data.select("pickup_datetime","dropoff_datetime").show(5)
````
## Step 9
### Describing the columns
Often when we are working with numeric features, we want to have a look at the statistics regarding the dataframe.
````
data.describe().show()
````
## Step 10
### To  import all PySpark Sql types libraries
````
from pyspark.sql.types import *
````
## Step 11
Converting into timestamp
````
data_conv = data.withColumn("pickup_datetime", data["pickup_datetime"].cast(TimestampType()))

data_conv2 = data_conv.withColumn("dropoff_datetime", data_conv["dropoff_datetime"].cast(TimestampType()))

data_conv2.printSchema()
````
## Step 12
To  import all PySpark Sql functions
````
import pyspark.sql.functions as f
````
## Step 13
New columns to show the pickup and dropoff days. \
``EEEE`` stands for showing the complete name of the day.
````
data_conv3 = data_conv2.withColumn("pickup_day", f.date_format("pickup_datetime", "EEEE"))

data_conv4 = data_conv3.withColumn("dropoff_day", f.date_format("dropoff_datetime", "EEEE"))

data_conv4.show()
````
## Step 14
New columns to show the pickup and dropoff day. \
``F`` stands for showing the day of the week in numbers and we convert the columns into an IntegerType
````
data_conv5 = data_conv4.withColumn("pickup_day_no", f.date_format("pickup_datetime", "F").cast(IntegerType()))

data_conv6 = data_conv5.withColumn("dropoff_day_no", f.date_format("dropoff_datetime", "F").cast(IntegerType()))

data_conv6.printSchema()
````
## Step 15
New columns to show the pickup and dropoff hours \
``H`` stands for showing the hour of the day and we convert the columns into an IntegerType
````
data_conv7 = data_conv6.withColumn("pickup_hour", f.date_format("pickup_datetime", "H").cast(IntegerType()))

data_conv8 = data_conv7.withColumn("dropoff_hour", f.date_format("dropoff_datetime", "H").cast(IntegerType()))

data_conv8.printSchema()
````
## Step 16
New columns to show the pickup and dropoff months \
``M`` stands for showing the pickup and dropoff months and we convert the columns into IntegerType

````
data_conv9 = data_conv8.withColumn("pickup_month", f.date_format("pickup_datetime", "M").cast(IntegerType()))

data_conv10 = data_conv9.withColumn("dropoff_month", f.date_format("dropoff_datetime", "M").cast(IntegerType()))

data_conv10.printSchema()
````
## Step 17
A User Defined function to calculate time of day per 4-hour period \
Since we are going to iterate over the dataset, we use the lambda function to do iterations

````
def time_of_day(x):
    if x in range(6,10):
        return "Morning"
    elif x in range(10,14):
        return "Afternoon"
    elif x in range(14,18):
        return "Evening"
    else:
        return "Night"

col_time_of_day = udf(lambda z: time_of_day(z))
spark.udf.register("col_time_of_day", time_of_day, StringType())
````
## Step 18
New columns to show the pickup and dropoff time of the day
````
data_conv11 = data_conv10.withColumn("pickup_timeofday", col_time_of_day("pickup_hour"))

data_conv12 = data_conv11.withColumn("dropoff_timeofday", col_time_of_day("dropoff_hour"))

data_conv12.show()

data_conv12.printSchema()
````

## Step 19
A User Defined function to calculate the distance between two coordinates
````
def cal_distance(pickup_lat , pickup_long , dropoff_lat, dropoff_long):
    start_coordinates = pickup_lat, pickup_long
    stop_coordinates = dropoff_lat, dropoff_long

    return great_circle(start_coordinates, stop_coordinates).km

cal_distance_udf = udf(lambda x1,x2,y1,y2: cal_distance(x1,x2,y1,y2))
spark.udf.register("cal_distance_udf", cal_distance, DoubleType())
````
## Step 20
New column to show the distances in km of trips
````
data_conv13 = data_conv12.withColumn("distance", cal_distance_udf("pickup_latitude", "pickup_longitude", "dropoff_latitude", "dropoff_longitude"))

data_conv13.show()
````

## Step 21
Now, we can import SparkSession from pyspark.sql and create a SparkSession, which is the entry point to Spark. \
You can give a name to the session using appName() and add some configurations with config() if you wish.

````
spark = SparkSession.builder\
        .master("local[*]")\
        .appName("New York Cab Info")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()
print("A Technical Case Study of New York Cab Trips")
````
To print the details concerning Spark
````
spark
````

## Step 22
Spark Sql query to display the total trips using id column
````
data_conv13.createOrReplaceTempView("data_conv13")
spark.sql("SELECT COUNT(id) AS total_trip from data_conv13").show()
````
## Step 23
To show the number of trips made according to the day of the week using id and vendor_id columns
````
spark.sql("SELECT pickup_day, COUNT(id) AS total_trips FROM data_conv13 GROUP BY pickup_day ORDER BY total_trips DESC").show()
````
````
spark.sql("SELECT pickup_day, COUNT(vendor_id) AS total_trips FROM data_conv13 GROUP BY pickup_day ORDER BY total_trips DESC").show()
````
## Step 24
To show the number of trips made according to the time of day using id and vendor_id columns
````
spark.sql("SELECT pickup_timeofday, COUNT(vendor_id) AS total_trips_time_of_day FROM data_conv13 GROUP BY pickup_timeofday ORDER BY total_trips_time_of_day DESC").show()
````
````
spark.sql("SELECT pickup_timeofday, COUNT(id) AS total_trips_time_of_day FROM data_conv13 GROUP BY pickup_timeofday ORDER BY total_trips_time_of_day DESC").show()
````
## Step 25
To show the number of km traveled per day of the week
````
spark.sql("SELECT pickup_day, SUM(distance) AS km_traveled_per_day FROM data_conv13 GROUP BY pickup_day ORDER BY km_traveled_per_day DESC").show()
````
## Step 26
### Save to file
Finally, after doing all the analysis and we want to save the results into a new CSV file
````
data_conv13.write.csv("/content/drive/MyDrive/New-York-Data/processed_final_data.csv", header=True)
````
But there is a catch here. There won’t be just a single CSV saved but multiple depending on the number of partitions of the dataframe. \
So if there are 2 partitions, then there will be two CSV files saved for each partition.
````
data_conv13.rdd.getNumPartitions()
````

## Conclusion
In this PySpark project, we were able to do some analysis on our dataset and query the dataset.
