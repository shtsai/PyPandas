#!/bin/bash

#Set up Python environment
ENV_DIR="env"
if [ ! -d $ENV_DIR ]
then
    mkdir $ENV_DIR
    python3 -m venv $ENV_DIR
fi
