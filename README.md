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
- Python 3.10 or greater
- Mac or Linux environment
- .env file in the root directory with correct credentials
- run `python -r requirement` from the terminal to install dependencies
### Run Instructions (from root directory)

- Full end-to-end process (extract/load into S3, transform/load all Postgres tables)
  - run `python src/code/main.py`
- **Only** extract/load into S3
  - run `python src/code/upload_s3.py`
- **Only** transform/load all tables into Postgres
  - run `python src/code/upload_postgres.py -a`
- **Only** transform/load a specific table into Postgres
  - run `python src/code/upload_postgres.py -t [TABLE NAME]`
    - See table names in Postgres Table Schema File (TBA)



