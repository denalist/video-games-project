# video-games-project

Project Objective
Build a foundational understanding of AWS services through a practical project focusing on data processing, automation, and basic AWS integrations, then moving into machine learning applications.

Dataset: https://www.kaggle.com/datasets/maso0dahmed/video-games-data

Weekend 1: Data Preparation and Basic AWS Lambda Integration (3rd Dec 23) 
Data Preparation
Split the video game dataset into individual file based on specific criteria (year).

AWS S3 Setup
Set up an S3 bucket to store these individual files and any future outputs.

AWS Lambda Function
Create a Lambda function for basic file processing tasks, like renaming files or simple data cleaning. This should also save to the s3 bucket under a diff path (eg s3://bucketname/output/)

Lambda and S3 Integration
Configure the Lambda function to trigger upon file uploads to the S3 bucket e.g. s3:://bucketname/input. Test this by uploading data. 

IAM Role
Create an IAM role to run this lambda as. It should only have access to the S3 bucket created.