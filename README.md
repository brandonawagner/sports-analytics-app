# Sports Analytics Application - College Football

This project contains source code and supporting files 
for analysis of historical American sports data. 

This application does the following

1. **Extracts** from the Sports Statistics (https://sports-statistics.com/) data set into an S3 bucket
2. **Loads** and stores the raw data into AWS Athena
3. **Transforms** and **Loads** the data into a PostgreSQL database.
4. Utilizes Metabase for the querying and data analysis UI

The project includes the following files and folders:
- `src/code` - Code for the application
- `src/files` - Local files shipped with applications


### Analyst Instructions for Using Application
This application is hosted in an EC2 instance and can be found
at http://brandonawagner.com:3001
- I do temporarily disable the database at times so let me know if you are trying to view the application
- See database table schema [here](./src/files/sa_prod_ERD.png)

Create a username and password then you will be able to use the
Sports Analytics Application database to analyze history college football
data.

You will want to use the Sports Analytics Application database.
Reach out to me if you prefer to use a test login instead of
creating your own.

### Developer Instructions


##### For Starting Application Locally
#### Pre-requisites
- Docker
- Mac or Linux environment
  - (This has been tested on Mac and Ubuntu)
- .env file in the root directory with correct credentials
- run `docker compose up` from the terminal to install dependencies
  - may need to use sudo

_(from project root directory)_

- Full end-to-end process (extract/load into S3, transform/load all Postgres tables)
  - run `python src/code/main.py`
- **Only** extract/load into S3
  - run `python src/code/upload_s3.py`
- **Only** transform/load all tables into Postgres
  - run `python src/code/upload_postgres.py -a`
- **Only** transform/load a specific table into Postgres
  - run `python src/code/upload_postgres.py -t [TABLE NAME]`



