from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.functions import col, length, regexp_replace

def info():
    print("Load library successfully!")

def load_data_job():
    '''Short cut function for loading DOB_Job_Application_Filings dataset'''
    spark = SparkSession.builder.appName("Test").config("spark.some.config.option", "some-value").getOrCreate()
    datafile = "DOB_Job_Application_Filings.csv"
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(datafile)
    df = clean_column_names(df)
    columns_to_clean = ["Initial Cost", "Total Est Fee"]
    df = cast_to_double(remove_char(df, columns_to_clean, "$"), columns_to_clean)
    return df 

def load_data_311():
    '''Short cut function for loading 311_Service_Requests dataset'''
    spark = SparkSession.builder.appName("Test").config("spark.some.config.option", "some-value").getOrCreate()
    datafile = "311_Service_Requests.csv"
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(datafile)
    df = clean_column_names(df)
    df = drop_null(df, "Latitude")
    df = drop_null(df, "Longitude")
    return df 

def load_data_permit():
    '''Short cut function for loading DOB_Permit_Issuance dataset'''
    spark = SparkSession.builder.appName("Test").config("spark.some.config.option", "some-value").getOrCreate()
    datafile = "DOB_Permit_Issuance.csv"
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(datafile)
    df = clean_column_names(df)
    df = drop_null(df, "LATITUDE")
    df = drop_null(df, "LONGITUDE")
    return df 

def drop_null(df, column):
    '''Drop rows that have null value in the given row'''
    return df.where(col(column).isNotNull())

def cast_to_int(df, columns):
    '''Convert a column type to integer, drop columns that are not convertible'''
    if type(columns) is str:
        df = df.withColumn(columns, df[columns].cast(IntegerType()))
        return drop_null(df, columns)
    elif type(columns) is list:
        for column in columns:
            df = df.withColumn(column, df[column].cast(IntegerType()))
            df = drop_null(df, column)
        return df
    else:
        raise ValueError("Invalid columns, use str or str list")

def cast_to_double(df, columns):
    '''Convert a column type to double, drop columns that are not convertible'''
    if type(columns) is str:
        df = df.withColumn(columns, df[columns].cast(DoubleType()))
        return drop_null(df, columns)
    elif type(columns) is list:
        for column in columns:
            df = df.withColumn(column, df[column].cast(DoubleType()))
            df = drop_null(df, column)
        return df
    else:
        raise ValueError("Invalid columns, use str or str list")

def cast_to_string(df, columns):
    '''Convert a column type to string, drop columns that are not convertible'''
    if type(columns) is str:
        df = df.withColumn(columns, df[columns].cast(StringType()))
        return drop_null(df, columns)
    elif type(columns) is list:
        for column in columns:
            df = df.withColumn(column, df[column].cast(StringType()))
            df = drop_null(df, column)
        return df
    else:
        raise ValueError("Invalid columns, use str or str list")

def limit_length(df, column, k):
    '''Filter out all rows whose column does not have length k. '''
    return df.where(length(col(column)) == k)

def remove_char(df, columns, char):
    '''Remove all occurrence of char from the given column(s)'''
    if len(char) != 1:
        raise ValueError("Invalid character, must have length 1 ") 
    # precede special characters with a backslash
    if char in ['*', '+', '?', '\\', '.', '^', '[', ']', '$', '&', '|']:
        char = '\\' + char

    if type(columns) is str:
        return _remove_char(df, columns, char)
    elif type(columns) is list:
        for column in columns:
            df = _remove_char(df, column, char)
        return df
    else:
        raise ValueError("Invalid columns, use str or str list")

def _remove_char(df, column, char):
    '''Remove all occurrence of char from the given column'''
    return df.withColumn(column, regexp_replace(column, char, ""))

def clean_column_names(df):
    '''Remove dot (.) in column names'''
    columns = df.columns
    for column in columns:
        if "." in column:
            newname = column.replace(".", "")
            df = df.withColumnRenamed(column, newname)
    return df
