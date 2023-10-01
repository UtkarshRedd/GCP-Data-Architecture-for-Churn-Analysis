# GCP-Data-Architecture-for-Churn-Analysis
This repository contains partial code for the Churn Analysis Project I conducted at Accenture. The outline of the project is as follows: -

1. Different types of telecommunications data, including Call Detail Records, Transactions, Internet Usage, and Customer Data, are being loaded as JSON files at regular intervals in a Google Cloud Storage (GCS) Bucket. 
2. Similar data is also being ingested into the Google Cloud environment using Cloud Pub/Sub.
3. The task is to establish ETL pipelines that will efficiently consolidate data coming from all sources into GCP BigQuery in separate tables for CDR, Transactions, Internet usage, and customer data.


# Solution Design
1. For the data being loaded into GCS, a cloud function has been developed that will get triggered every time a new JSON file is loaded into the bucket. Its respective path is dynamically determined and is appended to its respective BigQuery table. This is available in the ingestion_cloud_function.py file.
2. For the data being transmitted through GCP Pub/Sub, a subscription was created to obtain messages from the particular topic, and a pipeline was built using GCP Dataflow to automatically load the data from the GCP Publisher to the respective BigQuery tables. This part of the architecture could not be made available in this GitHub repository.
3. Extensive feature engineering was conducted to answer several specific questions provided by the client, which are available in the Queries.sql file. 
4. The final tables loaded into GCP BigQuery were analyzed to predict customer churn and the main reasons behind high churn rates.
5. The data is confidential and hence is not available. Therefore, a similar churn prediction model has been built on sample data for this repository.

Furthermore, the jupyter notebooks contain exploratory code for analyzing the different types of telecommunications data in this project.
