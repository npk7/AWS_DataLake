# Data Lakes using Amazon AWS and Apache Spark

## Executive Summary

Sparkify has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I built an ETL Pipeline that extracts their data from S3, processses them using Spark and loads them back into S3 as a set of dimensional tables.

## How to run the script

To execute the ETL pipeline, please run:

python3 etl.py

## Files

- etl.py - the ETL pipeline that extracts data, processes them and loads into S3
- dl.cfg - data lake configuration file with 
- README.md
