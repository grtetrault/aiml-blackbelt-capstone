#!/bin/bash
sudo yum update -y && sudo yum upgrade -y  
sudo yum install -y python-devel python3-devel libjpeg-devel

sudo python3 -m pip install \
    pandas==1.1.5           \
    pyarrow==4.0.1          \
    scipy==1.5.2            \
    scikit-learn==0.22.1    \
    matplotlib==3.2.2       \
    seaborn==0.11.2         \
    statsmodels==0.12.1     \
    mleap==0.17.0