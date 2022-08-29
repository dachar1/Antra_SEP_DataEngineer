# Databricks notebook source
# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %run ./includes/operations

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/dbfs/FileStore/movie/bronze/p_ingestdate=2022-08-25"))

# COMMAND ----------

rawDF = read_batch_raw(rawPath)

# COMMAND ----------

transformedRawDF = transform_raw(rawDF)

# COMMAND ----------

from pyspark.sql.types import *

assert transformedRawDF.schema == StructType(
    [
        StructField("datasource", StringType(), False),
        StructField("ingesttime", TimestampType(), False),
        StructField("status", StringType(), False),
        StructField("value", StringType(), True),
        StructField("p_ingestdate", DateType(), False),
    ]
)
print("Assertion passed.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_bronze

# COMMAND ----------

bronzeDF = spark.read.table("movie_bronze").filter("status = 'new'")

# COMMAND ----------

from pyspark.sql.functions import from_json

json_schema = """
    BackdropUrl STRING
    Budget FLOAT,
    CreatedBy STRING,
    CreatedDate TIMESTAMP,
    Id LONG,
    ImdbUrl STRING,
    OriginalLanguage STRING,
    Overview STRING,
    PosterUrl STRING,
    Price FLOAT,
    ReleaseDate TIMESTAMP,
    Revenue FLOAT,
    RunTime INTEGER,
    Tagline STRING,
    Title STRING,
    TmdbUrl STRING,
    UpdatedBy STRING,
    UpdatedDate TIMESTAMP,
    genres ARRAY,
    element STRUCT
    id LONG,
    name STRING
"""

bronzeAugmentedDF = bronzeDF.withColumn("nested_json", from_json(col("col"), json_schema)
)

# COMMAND ----------


