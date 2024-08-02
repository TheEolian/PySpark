# Using row_number with a Window Specification in PySpark

### Introduction


In PySpark, the row_number function is used to assign a unique sequential integer to rows within a window partition. This can be particularly useful for tasks such as ranking, deduplication, or window-based calculations.

This guide explains how to use the row_number function along with a window specification to create a new column in a DataFrame. We'll walk through a step-by-step example to demonstrate the process.


### Step-by-Step Example

#### Step 1: Initialize Spark Session

First, we need to initialize a Spark session.

```python
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

```
#### Step 2: Create Sample Data

Next, we'll create some sample data to work with.

```python
# Sample data
data = [
    ("offer1", "2023-08-01 10:00:00"),
    ("offer1", "2023-08-01 09:00:00"),
    ("offer1", "2023-08-01 11:00:00"),
    ("offer2", "2023-08-02 10:00:00"),
    ("offer2", "2023-08-02 09:00:00"),
]

# Create DataFrame
df = spark.createDataFrame(data, ["offr_id", "transaction_dt_time"])
```

#### Step 3: Convert String to Timestamp

We need to convert the transaction_dt_time column from a string to a timestamp.

```python
from pyspark.sql.functions import col

# Convert string to timestamp
df = df.withColumn("transaction_dt_time", col("transaction_dt_time").cast("timestamp"))
```

#### Step 4: Define Window Specification

Define the window specification to partition the data by offr_id and order by transaction_dt_time in descending order.

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Define window specification
window_spec = Window.partitionBy("offr_id").orderBy(col("transaction_dt_time").desc())
```

#### Step 5: Add Row Number Column

Use the row_number function with the window specification to add a new column called row_num.

```python
# Add row number column
df_with_row_num = df.withColumn("row_num", row_number().over(window_spec))
```

#### Step 6: Show the Result

Display the resulting DataFrame.

```python
# Show the result
df_with_row_num.show()
```

### Complete Code Example

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Sample data
data = [
    ("offer1", "2023-08-01 10:00:00"),
    ("offer1", "2023-08-01 09:00:00"),
    ("offer1", "2023-08-01 11:00:00"),
    ("offer2", "2023-08-02 10:00:00"),
    ("offer2", "2023-08-02 09:00:00"),
]

# Create DataFrame
df = spark.createDataFrame(data, ["offr_id", "transaction_dt_time"])

# Convert string to timestamp
df = df.withColumn("transaction_dt_time", col("transaction_dt_time").cast("timestamp"))

# Define window specification
window_spec = Window.partitionBy("offr_id").orderBy(col("transaction_dt_time").desc())

# Add row number column
df_with_row_num = df.withColumn("row_num", row_number().over(window_spec))

# Show the result
df_with_row_num.show()
```

### Expected Output

```python
+-------+-------------------+-------+
|offr_id|transaction_dt_time|row_num|
+-------+-------------------+-------+
| offer1|2023-08-01 11:00:00|      1|
| offer1|2023-08-01 10:00:00|      2|
| offer1|2023-08-01 09:00:00|      3|
| offer2|2023-08-02 10:00:00|      1|
| offer2|2023-08-02 09:00:00|      2|
+-------+-------------------+-------+
```

#### Explanation


withColumn("row_num", ...):

- Creates a new column named row_num.


row_number().over(...):

- Assigns a sequential integer to rows within a window partition.


Window.partitionBy("offr_id"):

- Partitions the data by the offr_id column.

.orderBy(col("transaction_dt_time").desc()):

-Orders rows within each partition by the transaction_dt_time column in descending order.
By following these steps, you can effectively use the row_number function with a window specification to add a new column to your DataFrame in PySpark.

### Additional Resources
[PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
