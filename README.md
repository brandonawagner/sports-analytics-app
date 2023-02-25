# Sports Analytics Application - College Football

This project contains source code and supporting files 
for analysis of historical American sports data. 

This serverless
application extracts from the Sports Statistics data set, loads into
an S3 bucket and transforms it to load into a PostgreSQL database.
This data is then displayed using Metabase

The project includes the following files and folders:

- `src` - Code for the application's Lambda functions.
- `template.yml` - A SAM template that defines the application's AWS resources.
- `buildspec.yml` -  A build specification file that tells AWS CodeBuild how to create a deployment package for the function.

