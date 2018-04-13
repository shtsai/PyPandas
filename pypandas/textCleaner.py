import re
import pyspark.sql
from pyspark.sql.functions import udf

def clean_with_pattern(dataframe, column, to_replace, value):

    def re_sub_function(datum):
        if datum is not None:
            return re.sub(to_replace, value, datum)
        else:
            return None

    clean_udf = udf(re_sub_function)
    return dataframe.withColumn(column, clean_udf(column))
