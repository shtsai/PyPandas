#!/bin/bash

# Create directory for data if not exist
DATA_DIR="../data"
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
