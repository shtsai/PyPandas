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

# Instantiate KMeans Outlier Remover
km = KMeansOutlierRemover()

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
km.summary()
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
km.show_cluster(3)
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
