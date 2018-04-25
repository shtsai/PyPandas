from pyspark.sql import SparkSession
from pypandas.preprocess import *

def load_data_job(where):
    '''Short cut function for loading DOB_Job_Application_Filings dataset'''
    if where == "aws":
        datafile = "s3://emrbucket-st3127/DOB_Job_Application_Filings.csv"
    elif where == "dumbo":
        datafile = "DOB_Job_Application_Filings.csv"
    else:
        raise ValueError("Invalid argument, choose either aws or dumbo")

    spark = SparkSession.builder.appName("Test").config("spark.some.config.option", "some-value").getOrCreate()
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(datafile)
    df = clean_column_names(df)
    columns_to_clean = ["Initial Cost", "Total Est Fee"]
    df = cast_to_double(remove_char(df, columns_to_clean, "$"), columns_to_clean)
    df = cast_to_int(df, ["Block", "Lot", "Community - Board", "Applicant License #"])
    return df 

def load_data_311(where):
    '''Short cut function for loading 311_Service_Requests dataset'''
    if where == "aws":
        datafile = "s3://emrbucket-st3127/311_Service_Requests.csv"
    elif where == "dumbo":
        datafile = "311_Service_Requests.csv"
    else:
        raise ValueError("Invalid argument, choose either aws or dumbo")

    spark = SparkSession.builder.appName("Test").config("spark.some.config.option", "some-value").getOrCreate()   
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(datafile)
    df = clean_column_names(df)
    df = drop_null(df, "Latitude")
    df = drop_null(df, "Longitude")
    return df 

def load_data_permit(where):
    '''Short cut function for loading DOB_Permit_Issuance dataset'''
    if where == "aws":
        datafile = "s3://emrbucket-st3127/DOB_Permit_Issuance.csv"
    elif where == "dumbo":
        datafile = "DOB_Permit_Issuance.csv"
    else:
        raise ValueError("Invalid argument, choose either aws or dumbo")

    spark = SparkSession.builder.appName("Test").config("spark.some.config.option", "some-value").getOrCreate()
    datafile = "s3://emrbucket-st3127/DOB_Permit_Issuance.csv"
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(datafile)
    df = clean_column_names(df)
    df = drop_null(df, "LATITUDE")
    df = drop_null(df, "LONGITUDE")
    return df 


