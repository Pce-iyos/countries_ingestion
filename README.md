# Countries_Information_Ingestion

This project was implemented using various tools including python, PostgresSQL, Streamlit and Apache Airflow

The project involves the following steps:

1. Extraction of country information data from a public REST API using the requests library in python.
2. The extracted data undergoes necessary transformations 
3. Loading the transformed data into a postgresSQL database by creating the necessary fields required.
4. Using the loaded data, several analyses and visualisation were conducted to answer the questions asked in the project, using streamlit.
5. Apache Airflow was used as the ochestration tool to schedule and manage the entire ETL process
6. A notification was sent to a discord server whenever the data was extracted and when it was loaded to postgres


## Data Architecture

![Screenshot 2024-07-23 194043](https://github.com/user-attachments/assets/db2a61ce-e4a2-4c95-a9c7-7f6bbd4dd10a)
