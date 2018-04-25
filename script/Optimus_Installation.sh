#!/bin/bash

#install python 3.6
sudo bash -c 'yum install python36'
wait
echo '[+]Complete install Python 3.6'

#install optimus
sudo bash -c 'python3 -m pip install optimuspyspark'
wait
echo '[+]Completed the installation of Optimus.'

#upgrade pip to the newest verison
sudo bash -c 'python3 -m pip install --upgrade pip'
wait
echo '[+]Complete upgrade the pip.'

#edit bashrc
var1=$(grep "export PYSPARK_PYTHON=python3" ~/.bashrc)
if [ -z "$var1" ]
then
    echo -e "\nexport PYSPARK_PYTHON=python3" | sudo tee -a  ~/.bashrc
fi
var2=$(grep "export PYSPARK_DRIVER_PYTHON=python3" ~/.bashrc)
if [ -z "$var2" ]
then
    echo "export PYSPARK_DRIVER_PYTHON=python3"  | sudo tee -a  ~/.bashrc
fi
wait

echo '[+]Complete adding variables to bashrc.'
source ~/.bashrc
#add the pythob tkinter
sudo bash -c "sed -i -e 's/^backend.*TkAgg$/backend : agg/g' /usr/local/lib64/python3.6/site-packages/matplotlib/mpl-data/matplotlibrc"
#install ipython
sudo bash -c 'python3 -m pip install ipython'
wait
echo '[+]Complete install iPython Package.'
echo '[+]Congrats. You have completed the installation and setup for Optimus.'
