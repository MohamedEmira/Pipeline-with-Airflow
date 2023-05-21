# Pipeline-with-Airflow

This project is an example of a data integration pipeline that transfers data from RDD (Relational Database) tables to S3 using Apache Airflow. The pipeline includes tasks for extracting data from RDD tables, transforming and detecting new or changed rows, and loading the data into Snowflake Data Warehouse.

## Project Structure

The project includes the following files:

- `Pipeline.py`: The main Airflow DAG file that defines the data integration workflow.
- `emp_dim_insert_update.py`: A Python module containing the function for joining and detecting new or changed rows.
- `queries.py`: A Python module containing SQL queries used in the data integration process.

## Prerequisites

Before running the RDD to S3 pipeline, ensure that the following prerequisites are met:

1. Apache Airflow is installed and properly configured.
2. AWS S3 and Snowflake accounts are set up, and the necessary credentials are available.
3. The required Python libraries are installed. You can use `pip` to install the dependencies specified in the `requirements.txt` file.

