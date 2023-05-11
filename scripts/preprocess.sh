#!/bin/bash
#pip install -r requirements.txt
rm -r data/
mkdir data/
python scripts/preprocess.py 
rm -r data/bigdataproject.zip
