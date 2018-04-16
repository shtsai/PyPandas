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
