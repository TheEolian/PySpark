# PySpark  Cheat Sheet for Pandas Users

### Importing Libraries

python
# Pandas
import pandas as pd

# PySpark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


### Creating DataFrames

python
# Pandas
df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})

# PySpark
spark = SparkSession.builder.appName('example').getOrCreate()
df = spark.createDataFrame([(1, 'a'), (2, 'b'), (3, 'c')], ['col1', 'col2'])


### Selecting Columns

python
# Pandas
df['col1']

# PySpark
df.select('col1')


### Filtering Rows

python
# Pandas
df[df['col1'] > 1]

# PySpark
df.filter(df['col1'] > 1)


### Grouping and Aggregating

python
# Pandas
df.groupby('col2').agg({'col1': 'sum'})

# PySpark
df.groupBy('col2').agg(F.sum('col1').alias('sum_col1'))


### Joining DataFrames

python
# Pandas
pd.merge(df1, df2, on='key')

# PySpark
df1.join(df2, on='key')


### Sorting DataFrames

python
# Pandas
df.sort_values(by='col1')

# PySpark
df.orderBy('col1')


### Handling Missing Values

python
# Pandas
df.dropna()

# PySpark
df.dropna()


### Applying Functions

python
# Pandas
df.apply(lambda x: x['col1'] * 2, axis=1)

# PySpark
df.withColumn('col1_double', df['col1'] * 2)


### Writing DataFrames to Files

python
# Pandas
df.to_csv('file.csv', index=False)

# PySpark
df.write.csv('file.csv', mode='overwrite', header=True)


### Convert to Pandas DataFrame

python
# Pandas
df_pandas = df.toPandas()

# PySpark
df_spark = spark.createDataFrame(df_pandas)

## Basic DataFrame Operations

### Display DataFrame

python
# Pandas
print(df)

# PySpark
df.show()


### DataFrame Shape

python
# Pandas
df.shape

# PySpark
(df.count(), len(df.columns))


## Handling Columns

### Renaming Columns

python
# Pandas
df.rename(columns={'old_name': 'new_name'}, inplace=True)

# PySpark
df.withColumnRenamed('old_name', 'new_name')


### Adding Columns

python
# Pandas
df['new_col'] = df['col1'] + df['col2']

# PySpark
df.withColumn('new_col', df['col1'] + df['col2'])


### Dropping Columns

python
# Pandas
df.drop(['col1', 'col2'], axis=1, inplace=True)

# PySpark
df.drop('col1', 'col2')


## Data Manipulation

### Casting Column Types

python
# Pandas
df['col1'] = df['col1'].astype(float)

# PySpark
df.withColumn('col1', df['col1'].cast('float'))


### Handling Duplicates

python
# Pandas
df.drop_duplicates(inplace=True)

# PySpark
df.dropDuplicates()


### Filling Missing Values

python
# Pandas
df.fillna(0, inplace=True)

# PySpark
df.fillna(0)


### Applying User-Defined Functions (UDFs)

python
# Pandas
def my_function(x):
    return x * 2

df['col1'] = df['col1'].apply(my_function)

# PySpark
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

def my_function(x):
    return x * 2

my_udf = udf(my_function, IntegerType())
df.withColumn('col1', my_udf(df['col1']))

## Aggregations and Window Functions

### Aggregations

python
# Pandas
df['col1'].sum()

# PySpark
df.agg(F.sum('col1')).collect()[0][0]


### Window Functions

python
# Pandas
df['rank'] = df['col1'].rank()

# PySpark
from pyspark.sql.window import Window

window = Window.orderBy('col1')
df.withColumn('rank', F.rank().over(window))


## Date and Time Operations

### Converting Strings to Timestamps

python
# Pandas
df['date'] = pd.to_datetime(df['date'])

# PySpark
df.withColumn('date', F.to_timestamp('date', 'yyyy-MM-dd HH:mm:ss'))


### Extracting Date Parts

python
# Pandas
df['year'] = df['date'].dt.year

# PySpark
df.withColumn('year', F.year('date'))


## Performance Tips

### Repartitioning DataFrames

python
# Pandas
# Not applicable

# PySpark
df.repartition(10)


### Repartitioning DataFrames

python
# Pandas
# Not applicable

# PySpark
df.repartition(10)


### Caching DataFrames

python
# Pandas
# Not applicable

# PySpark
df.cache()


### Broadcasting Small DataFrames
python
# Pandas
# Not applicable

# PySpark
from pyspark.sql.functions import broadcast

df1.join(broadcast(df2), 'key')


## Interfacing with SQL

### Register DataFrame as Temporary Table

python
# Pandas
# Not applicable

# PySpark
df.createOrReplaceTempView('table_name')
spark.sql('SELECT * FROM table_name')


### Running SQL Queries

python
# Pandas
# Not applicable

# PySpark
spark.sql('SELECT col1, SUM(col2) FROM table_name GROUP BY col1')


### Additional Resources
[PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)


### Additional Notes
PySpark operations are lazy; they are executed only when an action (like show(), collect(), write()) is called.
PySpark uses distributed computing, so operations can handle large datasets that do not fit into memory.
