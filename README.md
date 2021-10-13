# Sparkify Data pipeline
--- 
**This project is part of module 1 of udacity's data engineering course - relational databases, star schema and snow flake modeling.**

Pipeline for extracting, transforming, and loading data from the sparkify music streaming app into a database for analytics.

## Table of contents
* [General info](#general-info)
* [Technologies](#technologies)
* [Files](#files)
* [Setup](#setup)

## General info
A startup called Sparkify wants to analyze the data they’ve been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to.
This project performs the extraction and transformation of data coming from user logs and music plays from sparkify's music streaming app. In addition, we carry out the modeling of a dimensional database using star schema modeling. Finally, the project loads the transformed data in this database for further analysis of this information. The image below shows the data model.

![starschema](https://udacity-reviews-uploads.s3.us-west-2.amazonaws.com/_attachments/339318/1586016120/Song_ERD.png)


## Technologies
The project is created with:
* Python version: 3.8.8
* Jupyter lab: 3.0.14
* Amazon S3
* Amazon EMR 

## Files
The project contains this files
* etl.py: This file contains the code responsible for extracting data from data sources, transforming them and loading them into the Amazon S3.
* test_files.ipynb: This file contains the code responsible for testing the previous codes on some development platforms, such as the jupyter lab.

## Setup
To run this project locally:

```
$ cd ../[project directory path]
$ python etl.py
``` 