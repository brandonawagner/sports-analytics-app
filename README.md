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

### Pre-requisites
- Python 3.11 or greater
- Mac or Linux environment
- .env file in the root directory with correct credentials
- run `python3 -r requirement` from the terminal to install dependencies
### Run Instructions (from root directory)

- Full end-to-end process (extract/load into S3, transform/load into Postgres)
  - run `python3 src/code/main.py`
- **Only** run extract/load into S3
  - run `python3 src/code/upload_s3.py`
- **Only** run transform/load into Postgres
  - run `python3 src/code/upload_postgres.py`



