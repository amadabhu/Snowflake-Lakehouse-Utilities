# Snowflake_data_ingest
Set of ingest files and parsers for snowflake data warehouse.
This is not a complete application, rather my specific contributions. I replaced all the python scripts with the sql queries. The python scripts are incredibly inefficient, they served as proof of concept.

Later, I realized that Snowflake could directly query xml tags. These queries I then put into stored procedures using javascript. The stored procedures aren't included here.
