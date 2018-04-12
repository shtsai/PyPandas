#!/bin/bash

# Make local directary
prefix=$HOME"/.local"
if [ ! -d $prefix ]; then  
    mkdir $prefix
fi

# Install Python 3.6
bashrc=$HOME"/.bashrc"
my_pwd=$PWD
python_36_path=$prefix"/bin/python3.6"

if [ ! -f $python_36_path ]; then
    cd $HOME
    wget https://www.python.org/ftp/python/3.6.5/Python-3.6.5.tgz
    tar zxvf Python-3.6.5.tgz
    cd Python-3.6.5

    ./configure --prefix $prefix
    make -j 4
    make install

    echo "alias python36=\'"$python_36_path"\'" >> $bashrc
    source $bashrc
    cd $my_pwd
fi
