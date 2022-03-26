# city_of_boston_services


## Problem statement - 311 Service Request Analytics
(This is a hypothetical and synthetic requirement formulated for the zoomcamp project).

City of Boston wishes to review how its residents requested city services and how departments responded to those requests. The city also aim to identify areas of imporvement on its newly deployed 311 system and enhance the efficiency of City departments that deliver basic city services.


To solve this need, the data engineering / analytics need to create a pipeline to convert the raw data collected into actionable dashboards for the management team to analyze and curate to make informed decision.


KPI's
  1.  Simplify and shorten time between a resident reporting a problem and its resolution.
  2.  Enhance the 311 system to become an effective management tool, generating real-time reports that help departments manage staff, track trends,                   highlight and monitor performance, target efficiency needs and maximize its resources.
  3.  Determine the impact of the 311 system on 911 Emergency System (more efficient by diverting non-emergency calls that could impede the City's emergency response.)


The pipeline refreshes data on a monthly basis with 311 request information recieved, extracting the raw data into data lake first for storage, transforming the data and loading into data warehouse(bigQuery) for easier dashboard construction and analytics.

## Project high level design
This project produces a pipeline which:

1. Uses Terraform to manage the infrastructure
2. Extract the raw data into GCS in Google cloud
3. Transforms the raw data into standard tables using Apache Spark
4. Load the transformed data into BigQuery
5. repartition and Write the raw data into an archival storage in parquet format
6. Produce dashboard tiles in Google Data studio.
7. Ochestrate the pipeline using Airflow

> Architecture
> <img width="891" alt="image" src="https://user-images.githubusercontent.com/86935340/159110024-cab9b753-0b69-4314-9efb-9ad7e9650a2c.png">


## Dataset
[City of Boston Dataset](https://data.boston.gov/dataset/8048697b-ad64-4bfc-b090-ee00169f2323/resource/f53ebccd-bc61-49f9-83db-625f209c95f5/download/tmppgq9965_.csv)


## Technology choices
1. Cloud: GCP
2. Datalake: GCS Bucket
3. Infrastructure as code (IaC): Terraform 
4. Workflow orchestration: Airflow 
5. Data Warehouse: BigQuery 
6. Transformations: PySpark

## Proposal to address the requirements
1. **Data ingestion** - Using Airflow to download the dataset and place it in google cloud storage(GCS)
2. **Data warehouse** - BigQuery will be used to host the tables
3. **Transformations** - Use pyspark to transform the data from GCS bucket and add to BigQuery (partitioned and clustered)
4. **Dashboard** - Use Google Data studio to build the dashboards 

## Qustions answered
1. Most used channel for requesting service
2. Percent of ticket resolved ontime
3. Maximum no of days to resolve a ticket in each category
4. Most frequent request
5. Average resolution time of a case type

## Dashboard Tiles
## 
[Visualization](https://datastudio.google.com/reporting/d99a4623-74d2-40ee-a1f6-7e3180cb77e2)

