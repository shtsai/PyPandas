#!/bin/bash

#Set up Python environment
ENV_DIR="env"
if [ ! -d $ENV_DIR ]
then
    mkdir $ENV_DIR
    python36 -m venv $ENV_DIR
fi

source $ENV_DIR"/bin/activate"
