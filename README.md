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
```python
from pyspark.sql import SparkSession
from pypandas.outlier import KMeansOutlierRemover

# Create Spark Session
spark = SparkSession.builder.getOrCreate()

# Load your dataframe here
data = load_data()

df.count()
# 1508177  

df.show()
'''
+------------+-------------+
|Initial Cost|Total Est Fee|
+------------+-------------+
|         0.0|        130.0|
|         0.0|        320.0|
|         0.0|          0.0|
|         0.0|        100.0|
|         0.0|       1120.0|
|         0.0|        200.0|
|         0.0|        200.0|
|         0.0|        200.0|
|         0.0|        100.0|
|         0.0|      2230.28|
|         0.0|      1330.98|
|         0.0|        100.0|
|    125000.0|       992.21|
|         0.0|      2223.46|
|         0.0|      2223.46|
|         0.0|        260.0|
|         0.0|        408.0|
|         0.0|          0.0|
|         0.0|        538.0|
|         0.0|          0.0|
+------------+-------------+
only showing top 20 rows
'''

# Instantiate Outlier Remover from the factory
# Available choices are "kmeans", "bisectingkmeans", and "gaussian"
km = OutlierRemover.factory("kmeans")

# Check (default) parameters
km.k
# 3

# Set parameters based on user's knowledge on the dataset
km.set_param(k=5)
km.k
# 5

# Perform KMeans clustering
km.fit(df, ["Initial Cost", "Total Est Fee"])

# Get clustering summary
s = km.summary()
s.show()
'''
+-------------+-------+--------------------+-------------------------------+    
|cluster index|   size|      cluster center|avg(distance to cluster center)|
+-------------+-------+--------------------+-------------------------------+
|            0|1506910|   86153.79855969973|             115623.10175225197|
|            1|     11|3.7746609272727275E8|             9.78881126369113E7|
|            2|      3|        9.55543933E8|            3.703218131733947E7|
|            3|   1206|1.6625062420529801E7|              7289962.118664694|
|            4|     47|1.1278780606382978E8|           3.6478952700282976E7|
+-------------+-------+--------------------+-------------------------------+
'''

# Show a particular cluster
cluster3 = km.get_cluster(3)
cluster3.show()
'''
+----------+--------------------------+------------+-------------+
|prediction|distance to cluster center|Initial Cost|Total Est Fee|
+----------+--------------------------+------------+-------------+
|         3|         4775293.760860619|     1.185E7|     122143.5|
|         3|         2216883.564916434| 1.4408275E7|     148501.2|
|         3|         8191841.731979836|   8433633.0|      86958.7|
|         3|         8186474.485411539|   8439000.0|      87010.2|
|         3|         2627175.097229941|    1.3998E7|     144815.9|
|         3|        2543986.5908025797| 1.4081188E7|     145258.1|
|         3|        1993081.6867797726|    1.4632E7|    160384.12|
|         3|         6555151.707938737| 1.0070235E7|     103944.8|
|         3|        3181787.7064663703|  1.344342E7|     138741.7|
|         3|         4030253.117367908|    1.2595E7|     129942.0|
|         3|         6375299.876071498|       2.3E7|     237113.5|
|         3|        2640317.4489083528| 1.9265215E7|     198653.3|
|         3|      3.3505087823614437E7| 5.0128347E7|     516757.2|
|         3|       1.987601568180299E7|      3.65E7|     376163.5|
|         3|          7292308.55701616|   9333200.0|      88484.5|
|         3|      1.4124048718706062E7| 3.0748338E7|     316928.2|
|         3|        2971534.1636515674| 1.3653663E7|     140849.7|
|         3|         4475199.270002893|      2.11E7|     217543.5|
|         3|         8049959.945984376|   8575506.0|      88546.3|
|         3|        509944.38674310147|  1.713495E7|     176759.0|
+----------+--------------------------+------------+-------------+
only showing top 20 rows
'''

# Filter out some outlier in cluster 3
km.filter(3, 10000000)

# Get filtered dataframe
newdf = km.get_dataframe()

# Show new count, 165 rows were filtered out
newdf.count()
# 1508012

```

### Scaling and Normalization
```python
from pyspark.sql import SparkSession
from pypandas.scale import standard_scale, min_max_scale, max_abs_scale, normalize, unpack_vector

# Create a dataframe
df = spark.createDataFrame([(1, 1.22, 2.34, 1.1, 3.5),\
                            (2, -0.23, 5.23, 4.22, 2.34),\
                            (3, 0.67, 1.34, -3.56, 7.45)],\
                            ["id","col1","col2","col3","col4"]) 
df.show()
'''
+---+-----+----+-----+----+
| id| col1|col2| col3|col4|
+---+-----+----+-----+----+
|  1| 1.22|2.34|  1.1| 3.5|
|  2|-0.23|5.23| 4.22|2.34|
|  3| 0.67|1.34|-3.56|7.45|
+---+-----+----+-----+----+
'''

# Use the standard_scale() function
# Users could optionally set the variable withStd and withMean
# withStd: True by default. Scales the data to unit standard deviation.
# withMean: False by default. Centers the data with mean before scaling. It will build a dense output, so take care when applying to sparse input.

scaled_df = standard_scale(df, ["col1", "col2", "col3", "col4"])
scaled_df.show(3,False)
'''
+---+-----+----+-----+----+-------------------------------------------------------------------------------+
|id |col1 |col2|col3 |col4|scaled features                                                                |
+---+-----+----+-----+----+-------------------------------------------------------------------------------+
|1  |1.22 |2.34|1.1  |3.5 |[1.6666521513105603,1.1583732592894616,0.28094763789611055,1.3064886711618937] |
|2  |-0.23|5.23|4.22 |2.34|[-0.31420491377166304,2.5890137376426856,1.0778173017468968,0.8734809972910946]|
|3  |0.67 |1.34|-3.56|7.45|[0.9152925749000619,0.6633419519008028,-0.9092487190092305,2.78095445718746]   |
+---+-----+----+-----+----+-------------------------------------------------------------------------------+
'''

# Use the min_max_scale() function
# Users could optionally set the variables min and max
# min: 0.0 by default. Lower bound after transformation, shared by all features.
# max: 1.0 by default. Upper bound after transformation, shared by all features.

scaled_df = min_max_scale(df,  ["col1", "col2", "col3", "col4"])
scaled_df.show(3,False)
'''
+---+-----+----+-----+----+---------------------------------------------------------------+
|id |col1 |col2|col3 |col4|scaled features                                                |
+---+-----+----+-----+----+---------------------------------------------------------------+
|1  |1.22 |2.34|1.1  |3.5 |[1.0,0.25706940874035983,0.5989717223650386,0.2270058708414873]|
|2  |-0.23|5.23|4.22 |2.34|[0.0,1.0,1.0,0.0]                                              |
|3  |0.67 |1.34|-3.56|7.45|[0.6206896551724138,0.0,0.0,1.0]                               |
+---+-----+----+-----+----+---------------------------------------------------------------+
'''

# Use the max_abs_scale() function

scaled_df = max_abs_scale(df,  ["col1", "col2", "col3", "col4"])
scaled_df.show(3,False)
'''
+---+-----+----+-----+----+----------------------------------------------------------------+
|id |col1 |col2|col3 |col4|scaled features                                                 |
+---+-----+----+-----+----+----------------------------------------------------------------+
|1  |1.22 |2.34|1.1  |3.5 |[1.0,0.4474187380497131,0.26066350710900477,0.4697986577181208] |
|2  |-0.23|5.23|4.22 |2.34|[-0.18852459016393444,1.0,1.0,0.3140939597315436]               |
|3  |0.67 |1.34|-3.56|7.45|[0.5491803278688525,0.25621414913957935,-0.8436018957345972,1.0]|
+---+-----+----+-----+----+----------------------------------------------------------------+
'''

# Use the normalize() function
# Users could optionally set p-norm variable
# p-norm: 2 by default, used for normalization

normalized_df = normalize(df, ["col1", "col2", "col3", "col4"])
normalized_df.show(3,False)
'''
+---+-----+----+-----+----+--------------------------------------------------------------------------------+
|id |col1 |col2|col3 |col4|normalized features                                                             |
+---+-----+----+-----+----+--------------------------------------------------------------------------------+
|1  |1.22 |2.34|1.1  |3.5 |[0.26995379041977813,0.5177802209690826,0.2434009585752098,0.7744575954665766]  |
|2  |-0.23|5.23|4.22 |2.34|[-0.032304836487058015,0.7345838905535367,0.592723521632108,0.328666597303112]  |
|3  |0.67 |1.34|-3.56|7.45|[0.07984081144863289,0.15968162289726578,-0.4242287891897508,0.8877821571527089]|
+---+-----+----+-----+----+--------------------------------------------------------------------------------+
'''

# Use the unpack_vector() function
# The function unpack_vector() enables user unpack the dense vector into columns.

unpack_df=unpack_vector(normalized_df,"normalized features",["norm col1", "norm col2", "norm col3", "norm col4"])
unpack_df.show(3,False)
'''
+---+-----+----+-----+----+--------------------------------------------------------------------------------+
|id |col1 |col2|col3 |col4|normalized features                                                             |
+---+-----+----+-----+----+--------------------------------------------------------------------------------+
|1  |1.22 |2.34|1.1  |3.5 |[0.26995379041977813,0.5177802209690826,0.2434009585752098,0.7744575954665766]  |
|2  |-0.23|5.23|4.22 |2.34|[-0.032304836487058015,0.7345838905535367,0.592723521632108,0.328666597303112]  |
|3  |0.67 |1.34|-3.56|7.45|[0.07984081144863289,0.15968162289726578,-0.4242287891897508,0.8877821571527089]|
+---+-----+----+-----+----+--------------------------------------------------------------------------------+
                            |
                            |
                            v
+---+-----+----+-----+----+-----------+----------+----------+----------+
|id |col1 |col2|col3 |col4|norm col1  |norm col2 |norm col3 |norm col4 |
+---+-----+----+-----+----+-----------+----------+----------+----------+
|1  |1.22 |2.34|1.1  |3.5 |0.2699538  |0.51778024|0.24340096|0.7744576 |
|2  |-0.23|5.23|4.22 |2.34|-0.03230484|0.7345839 |0.59272355|0.3286666 |
|3  |0.67 |1.34|-3.56|7.45|0.07984081 |0.15968162|-0.4242288|0.88778216|
+---+-----+----+-----+----+-----------+----------+----------+----------+
'''
```
### Text Cleaning

```python
from pyspark.sql import SparkSession
from pypandas.text_cleaner import clean_text, sub_with_pattern, TextCleaner

# Create Spark Session
spark = SparkSession.builder.getOrCreate()

# Setup data
data = [(
    '    You can find the cheapest good on https://www.amazon.com/.    ',
    'See, this iPhoneX only cost $20 dollar!!! ',
    ' What the ______ :)) '
)]

df = spark.createDataFrame(data, schema=['1', '2', '3'])

df.show()
'''
+--------------------+--------------------+--------------------+
|                   1|                   2|                   3|
+--------------------+--------------------+--------------------+
|    You can find ...|See, this iPhoneX...| What the ______ ...|
+--------------------+--------------------+--------------------+
'''

# General cleaning feature
# Clean all column with '*' column
clean_text(df, '*').collect()
# [Row(
#     1='You can find the cheapest good on _url_',
#     2='See this iPhoneX only cost _number_ dollar',
#     3='What the _ '
# )]

# Clean specific columns
clean_text(df, ['1', '2']).collect()
# [Row(
#     1='You can find the cheapest good on _url_',
#     2='See this iPhoneX only cost _number_ dollar',
#     3=' What the ______ :)) '
# )]

# Customize your own cleaning ways
# For example, removing dollar sign
sub_with_pattern(df, ['2'], '\$', '').select('2').collect()
# [Row(2='See, this iPhoneX only cost 20 dollar!!! ')]

# You can apply your own regular expression in our framework
# For example, replacing consecutive white spaces to a single white space
sub_with_pattern(df, ['1'], ' +', ' ').select('1').collect()
# [Row(1=' You can find the cheapest good on https://www.amazon.com/. ')]

# To customize your cleaning process, you can use TextCleaner
cleaner = TextCleaner()
# First, remove leading space
cleaner.register_re_sub('^ +', '')
# Then, remove trailing space
cleaner.register_re_sub(' +$', '')
# Clean dataframe
cleaner.clean(df, ['1']).select('1').collect()
# [Row(1='You can find the cheapest good on https://www.amazon.com/.')]
```
