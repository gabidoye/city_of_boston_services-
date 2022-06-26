#!/usr/bin/env python
# coding: utf-8

import pyspark
from pyspark.sql import SparkSession
import warnings
warnings.filterwarnings('ignore')
import warnings; warnings.simplefilter('ignore')
import pandas as pd
from pyspark.sql.functions import *
from google.cloud import storage
from pyspark.sql import types
from pyspark.sql.functions import date_format


spark = SparkSession.builder.master("local").appName("Boston Service Request").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = types.StructType([
types.StructField('case_enquiry_id', types.StringType(),True),
types.StructField('open_dt', types.StringType(),True),
types.StructField('target_dt', types.TimestampType(),True),
types.StructField('closed_dt', types.TimestampType(),True),
types.StructField('ontime', types.StringType(),True),
types.StructField('case_status', types.StringType(),True),
types.StructField('closure_reason', types.StringType(),True),
types.StructField('case_title', types.StringType(),True),
types.StructField('subject', types.StringType(),True),
types.StructField('reason', types.StringType(),True),
types.StructField('type', types.StringType(),True),
types.StructField('queue', types.StringType(),True),
types.StructField('department', types.StringType(),True),
types.StructField('submittedphoto', types.StringType(),True),
types.StructField('closedphoto', types.StringType(),True),
types.StructField('location', types.StringType(),True),
types.StructField('fire_district', types.StringType(),True),
types.StructField('pwd_district', types.StringType(),True),
types.StructField('city_council_district', types.StringType(),True),
types.StructField('police_district', types.StringType(),True),
types.StructField('neighborhood', types.StringType(),True),
types.StructField('neighborhood_services_district', types.StringType(),True),
types.StructField('ward', types.StringType(),True),
types.StructField('precinct', types.StringType(),True),
types.StructField('location_street_name', types.StringType(),True),
types.StructField('location_zipcode', types.StringType(),True),
types.StructField('latitude', types.StringType(),True),
types.StructField('longitude', types.StringType(),True),
types.StructField('source', types.StringType(),True)
])


df = spark.read.option("header",True).schema(schema).csv("gs://datalake-311-bronze/boston_2022.csv")

df.write.format('bigquery').option('project','dataengineering-bizzy').option('table','boston_service_request.boston_311_raw').option("temporaryGcsBucket","datalake-311-silver").mode("overwrite").save()

df.write.format("parquet").option("path", "gs://datalake-311-silver/archive/").save() 

df = spark.read.format('bigquery').option('project','dataengineering-bizzy').option('table','boston_service_request.boston_311_raw').load()

df =df.drop("latitude", "longitude", "submittedphoto","police_district", "location_zipcode", "ward", "closedphoto","police_district", "neighborhood_services_district","queue" "fire_district","city_council_district","precinct", "location_street_name", "pwd_district")

df2 = df.replace('?',None)

df_time = df2.withColumn('resolution_time_sec',round(unix_timestamp("closed_dt")) - round(unix_timestamp('open_dt'))).withColumn('resolution_time_mins',round(unix_timestamp("closed_dt")/60) - round(unix_timestamp('open_dt')/60))

df_time.write.format('bigquery').option('project','dataengineering-bizzy').option('table','boston_service_request.boston_service_summary').option("temporaryGcsBucket","datalake-311-silver").mode("overwrite").save()

df2 =df_time.na.fill(value=0,subset=["resolution_time_sec", "resolution_time_mins"])

df2.createOrReplaceTempView("service_request")



spark.sql("select source, count(source) as Total from service_request group by source order by Total desc").show(truncate=False)



spark.sql("select ontime, count(ontime), SUM(COUNT(ontime)) OVER() AS total_count, count(ontime) *100.0 /sum(count(ontime)) over () as request_percent from service_request group by ontime").show(truncate=False)


spark.sql("select case_enquiry_id,open_dt,closed_dt,case_title, resolution_time_mins, (resolution_time_mins/60/24) as resolution_days from "\
          "(select * ,row_number() OVER (PARTITION BY type ORDER BY resolution_time_mins DESC) as rn "\
          " from service_request) tmp where rn <=1").show(truncate=False)


spark.sql("select case_title, count(case_title) as total from service_request group by case_title order by total desc").show(truncate=False)


spark.sql("select case_title, average_res_time_mins, (average_res_time_mins/60/24) as resolution_days from "\
          "(select * ,AVG(resolution_time_mins) OVER (PARTITION BY case_title ORDER BY case_title DESC) as average_res_time_mins "\
          "from service_request) group by case_title, average_res_time_mins,(average_res_time_mins/60/24) \
          order by case when case_title is null then 1 else 0 end, case_title ").show(truncate=False)

