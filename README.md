# PyPandas
PyPandas, a data cleaning framework for Spark

## Features
* Outlier detection
* Scaling and normalization
* Text cleaning

## Installation

`pip install pypandas`

## Usage

### Outlier Detection

### Scaling and Normalization
```python
from pyspark.sql improt SparkSession
from pypandas.scale improt *

# Create a dataframe
df = spark.createDataFrame([(1, 1.22, 2.34, 1.1, 3.5), (2, 0.23, 5.23, 4.22, 2.34), (3, 0.67, 1.34, 3.56, 7.45)], ["id","v1","v2","v3","v4"]) 

df.show()
'''
+---+----+----+----+----+
| id|  v1|  v2|  v3|  v4|
+---+----+----+----+----+
|  1|1.22|2.34| 1.1| 3.5|
|  2|0.23|5.23|4.22|2.34|
|  3|0.67|1.34|3.56|7.45|
+---+----+----+----+----+
'''
# Use the standard_scale() function
'''
withStd: True by default. Scales the data to unit standard deviation.
withMean: False by default. Centers the data with mean before scaling. It will build a dense output, so take care when applying to sparse input.
'''
scaled_df = standard_scale(df, ["v1", "v2", "v3", "v4"])
'''
Successfully scale the column 'v1' and create a new column 'scaled v1'.
Successfully scale the column 'v2' and create a new column 'scaled v2'.
Successfully scale the column 'v3' and create a new column 'scaled v3'.
Successfully scale the column 'v4' and create a new column 'scaled v4'.
'''
scaled_df.show()
'''
+---+----+----+----+----+----------+----------+---------+---------+
| id|  v1|  v2|  v3|  v4| scaled v1| scaled v2|scaled v3|scaled v4|
+---+----+----+----+----+----------+----------+---------+---------+
|  1|1.22|2.34| 1.1| 3.5| 2.4595907| 1.1583732|0.6689928|1.3064886|
|  2|0.23|5.23|4.22|2.34|0.46369335| 2.5890138|2.5664997| 0.873481|
|  3|0.67|1.34|3.56|7.45| 1.3507589|0.66334194|2.1651042|2.7809544|
+---+----+----+----+----+----------+----------+---------+---------+
'''
# Use the min_max_scale() function
'''
min: 0.0 by default. Lower bound after transformation, shared by all features.
max: 1.0 by default. Upper bound after transformation, shared by all features.
'''
scaled_df = min_max_scale(df, ["v1", "v2", "v3", "v4"])
'''
Successfully scale the column 'v1' to range (0.000000, 1.000000) and create a new column 'scaled v1'.
Successfully scale the column 'v2' to range (0.000000, 1.000000) and create a new column 'scaled v2'.
Successfully scale the column 'v3' to range (0.000000, 1.000000) and create a new column 'scaled v3'.
Successfully scale the column 'v4' to range (0.000000, 1.000000) and create a new column 'scaled v4'.
'''
scaled_df.show()
'''
+---+----+----+----+----+----------+---------+----------+----------+
| id|  v1|  v2|  v3|  v4| scaled v1|scaled v2| scaled v3| scaled v4|
+---+----+----+----+----+----------+---------+----------+----------+
|  1|1.22|2.34| 1.1| 3.5|       1.0|0.2570694|       0.0|0.22700587|
|  2|0.23|5.23|4.22|2.34|       0.0|      1.0|       1.0|       0.0|
|  3|0.67|1.34|3.56|7.45|0.44444445|      0.0|0.78846157|       1.0|
+---+----+----+----+----+----------+---------+----------+----------+
'''
# Use the max_abs_scale() function
scaled_df = max_abs_scale(df, ["v1", "v2", "v3", "v4"])
'''
Successfully scale the column 'v1' to range (-1, 1) and create a new column 'scaled v1'.
Successfully scale the column 'v2' to range (-1, 1) and create a new column 'scaled v2'.
Successfully scale the column 'v3' to range (-1, 1) and create a new column 'scaled v3'.
Successfully scale the column 'v4' to range (-1, 1) and create a new column 'scaled v4'.
'''
scaled_df.show()
'''
+---+----+----+----+----+----------+----------+---------+----------+
| id|  v1|  v2|  v3|  v4| scaled v1| scaled v2|scaled v3| scaled v4|
+---+----+----+----+----+----------+----------+---------+----------+
|  1|1.22|2.34| 1.1| 3.5|       1.0|0.44741875|0.2606635|0.46979865|
|  2|0.23|5.23|4.22|2.34|0.18852459|       1.0|      1.0|0.31409395|
|  3|0.67|1.34|3.56|7.45| 0.5491803|0.25621414|0.8436019|       1.0|
+---+----+----+----+----+----------+----------+---------+----------+
'''
# Use the normalize() function
'''
By defualt p-norm = 2 used for normalization
'''
normalized_df = normalize(df, ["v1", "v2", "v3", "v4"])
'''
Successfully assembled the column  'v1'  'v2'  'v3'  'v4'  to a feature vector and normalized using L^2.000000 norm and create two new columns 'feature' and 'normalized feature'.
'''
normalized_df.show(3,False)
'''
+---+----+----+----+----+---------------------+-------------------------------------------------------------------------------+
|id |v1  |v2  |v3  |v4  |features             |normalized features                                                            |
+---+----+----+----+----+---------------------+-------------------------------------------------------------------------------+
|1  |1.22|2.34|1.1 |3.5 |[1.22,2.34,1.1,3.5]  |[0.26995379041977813,0.5177802209690826,0.2434009585752098,0.7744575954665766] |
|2  |0.23|5.23|4.22|2.34|[0.23,5.23,4.22,2.34]|[0.032304836487058015,0.7345838905535367,0.592723521632108,0.328666597303112]  |
|3  |0.67|1.34|3.56|7.45|[0.67,1.34,3.56,7.45]|[0.07984081144863289,0.15968162289726578,0.4242287891897508,0.8877821571527089]|
+---+----+----+----+----+---------------------+-------------------------------------------------------------------------------+
'''
```
### Text Cleaning

```python
from pyspark.sql import SparkSession
from pypandas.textCleaner import clean_text, sub_with_pattern

# Create Spark Session
spark = SparkSession.builder.getOrCreate()

# Setup data
data = [('    abc$@#$     0079   $#@$SFSmck    ', 'Hi, I like https://www.google.com.', '_____a__c__ac')]
df = spark.createDataFrame(data)

df.show()
'''
+--------------------+--------------------+-------------+
|                  _1|                  _2|           _3|
+--------------------+--------------------+-------------+
|    abc$@#$     0...|Hi, I like https:...|_____a__c__ac|
+--------------------+--------------------+-------------+
'''

# General cleaning feature
# Clean all column with '*' column
clean_text(df, '*').collect()
# [Row(_1='abc _number_ SFSmck', _2='Hi I like _url_', _3='_a_c_ac')]

# Clean specific columns
clean_text(df, ['_1', '_2']).collect()
# [Row(_1='abc _number_ SFSmck', _2='Hi I like _url_', _3='_____a__c__ac')]

# Customize your own cleaning ways
# For example, removing stopwords I
sub_with_pattern(df, ['_2'], 'I', '').select('_2').collect()
# [Row(_2='Hi,  like https://www.google.com.')]

# You can apply your own regular expression in our framework
# For example, replacing consecutive white spaces to a single white space
sub_with_pattern(df, ['_1'], ' +', ' ').select('_1').collect()
# [Row(_1=' abc$@#$ 0079 $#@$SFSmck ')]
```
