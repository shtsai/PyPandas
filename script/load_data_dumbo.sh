#!/bin/bash
# This shell script will download the test data to the dumbo clusters.
# The datasets will first be downloaded to the /scracth directory, which is for large files on dumbo.
# After the download is completed, the datasets will be uploaded to the HDFS, so that you can access them in the Spark environment.

HADOOP_EXE='/usr/bin/hadoop'
HADOOP_LIBPATH='/opt/cloudera/parcels/CDH/lib'
HADOOP_STREAMING='hadoop-mapreduce/hadoop-streaming.jar'

hfs="$HADOOP_EXE fs"
hjs="$HADOOP_EXE jar $HADOOP_LIBPATH/$HADOOP_STREAMING"

# Create directory for data if not exist
DATA_DIR="$SCRATCH/data"
if [ ! -d "$DATA_DIR" ]
then
    mkdir $DATA_DIR
fi

# Download data if not exist
if [ ! -e $DATA_DIR/311_Service_Requests.csv ]
then
    echo "Downloading 311_Service_Requests.csv" 
    curl https://data.cityofnewyork.us/api/views/erm2-nwe9/rows.csv?accessType=DOWNLOAD --output $DATA_DIR/311_Service_Requests.csv -s &
fi

if [ ! -e $DATA_DIR/DOB_Permit_Issuance.csv ]
then
    echo "Downloading DOB_Permit_Issuance.csv" 
    curl https://data.cityofnewyork.us/api/views/ipu4-2q9a/rows.csv?accessType=DOWNLOAD --output $DATA_DIR/DOB_Permit_Issuance.csv -s &
fi

if [ ! -e $DATA_DIR/DOB_Job_Application_Filings.csv ]
then
    echo "Downloading DOB_Job_Application_Filings" 
    curl https://data.cityofnewyork.us/api/views/ic3t-wcy2/rows.csv?accessType=DOWNLOAD --output $DATA_DIR/DOB_Job_Application_Filings.csv -s &
fi

echo "Now the downloading job is running in the background"
echo "It will take approximately 10 mins to download all three files depending on your network speed"
echo "Please be patient"

wait

echo "All downloads completed!!"
echo "=========================================="
echo "Now uploading data to HDFS"

# upload data to hfs
echo "Uploading 311_Service_Requests.csv"
$hfs -test -e "311_Service_Requests.csv"
if [ $? -ne 0 ]; then
	$hfs -put $DATA_DIR/311_Service_Requests.csv 
fi
echo "Complete"

echo "Uploading DOB_Permit_Issuance.csv"
$hfs -test -e "DOB_Permit_Issuance.csv"
if [ $? -ne 0 ]; then
	$hfs -put $DATA_DIR/DOB_Permit_Issuance.csv
fi
echo "Complete"

echo "Uploading DOB_Job_Application_Filings.csv"
$hfs -test -e "DOB_Job_Application_Filings.csv"
if [ $? -ne 0 ]; then
	$hfs -put $DATA_DIR/DOB_Job_Application_Filings.csv
fi
echo "Complete"

echo "Your data is ready!"
