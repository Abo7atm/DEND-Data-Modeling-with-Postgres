# DEND Project #1: Data Modeling with Postgres
Data Modeling with Postgres is the first projec in Udacity's Data Engineer Nanodegree. The goal is to build a star schema optimized for queries on song play analysis.

### Discussion section
This database is designed to make the analysis for Sparkify easier. The database should be optimized for queries related to Sparkify's analysis. The star schema makes it easier to get insights from the data that Sparkify has.

The star schema is considered the simplest type of Data Warehousing schema, which is suitabe for such data that isn't complex. And it's easy to understand.
The pipeline is prepares the data before inserting into the database, in terms of converting timestamps to readable formats of time. Going over all the files ensures that all the data is inserted into the database.

## Usage
First, using pyhton3, run `python create_tables.py` to initialize the database.<br/>
Then run `python etl.py`, and you're all set.

