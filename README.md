# Snowflake Lakehouse
A generalized set of utilities for developing a data warehouse in Snowflake using AWS S3 as a primary staging environment. The AWS folder contains boiler plate code I developed to be reusable 
data ingestion functions. The SQL_queries contain queries I wrote as part of an ETL pipeline ingesting Refinitiv data. These queries are just examples; that need to be replaced with your own use case specific queries. This was designed as a migration from several source servers to s3 then to Snowflake staging are and then to specific schemas.


# The stack

- Python 3.10
- AWS
- Snowflake


# TODO:
- Convert utilities into a class
- Diagram data flow

