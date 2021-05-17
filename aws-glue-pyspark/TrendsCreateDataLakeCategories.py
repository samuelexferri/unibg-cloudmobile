###### TEDx-Load-Aggregate-Model

import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, array_join, lit

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

##### FROM FILES
tags_dataset_path = "s3://trends-test-1/tags_dataset.csv"

##### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##### START JOB CONTEXT AND JOB
sc = SparkContext()

glueContext = GlueContext(sc)
spark = glueContext.spark_session
    
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

##### READ INPUT FILES TO CREATE AN INPUT DATASET
tags_dataset = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(tags_dataset_path)
    
tags_dataset.printSchema()

##### FILTER ITEMS WITH NULL POSTING KEY
count_items = tags_dataset.count()
count_items_null = tags_dataset.filter("idx is not null").count()

print(f"Number of items from RAW DATA {count_items}")
print(f"Number of items from RAW DATA with NOT NULL KEY {count_items_null}")

##### CREATE THE AGGREGATE MODEL, GROUP CATEGORIES
tags_dataset_agg = tags_dataset.groupBy(col("tag").alias("_id")).agg(collect_list("idx").alias("video_idx"))
tags_dataset_agg.printSchema()

##### ADD GOOGLE API FIELD
tags_dataset_agg = tags_dataset_agg.withColumn("google_api_score", lit(0))

##### MONGODB
mongo_uri = "mongodb://cluster0-shard-00-00.zizid.mongodb.net:27017,cluster0-shard-00-01.zizid.mongodb.net:27017,cluster0-shard-00-02.zizid.mongodb.net:27017"

write_mongo_options = {
    "uri": mongo_uri,
    "database": "trends-test-mongodb",
    "collection": "tedx_data_categories",
    "username": "admin123",
    "password": "admin123",
    "ssl": "true",
    "ssl.domain_match": "false"}
from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(tags_dataset_agg, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)