#!/bin/bash

conda create --name simppl

source activate simppl

pip install -r requirements.txt

mkdir tweets

mkdir followers

mkdir follower_tweets

python api.py --file "all_urls.csv" --url_col "urls" 