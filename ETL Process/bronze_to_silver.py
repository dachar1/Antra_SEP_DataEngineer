# Databricks notebook source
# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %run ./includes/operations

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------


movies_bronze = spark.read.load(path = bronzePath)
display(movies_bronze)

# COMMAND ----------

movies_silver = movie_bronze_to_silver(movies_bronze).distinct()
display(movies_silver)

# COMMAND ----------

transformedRawDF = transform_raw(rawDF)

# COMMAND ----------

from pyspark.sql.types import _parse_datatype_string

assert transformedRawDF== _parse_datatype_string(
    """
    movie string,
    BackdropUrl string,
    Budget double,
    CreatedBy timestamp,
    CreatedDate string,
    Id long,
    ImdbUrl string,
    Overview string,
    PosterUrl string,
    Price double,
    ReleaseTime timestamp,
    ReleaseDate date,
    ReleaseYear date,
    Revenue double,
    RunTime long,
    Tagline string,
    Title string,
    TmdbUrl string,
    UpdatedBy timestamp,
    UpdatedDate timestamp,
    movie_genre_junction_id long,
    Language_Id long
    """
)
print("Assertion passed.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_bronze

# COMMAND ----------

bronzeDF = spark.read.table("movie_bronze").filter("status = 'new'")

# COMMAND ----------

from pyspark.sql.types import _parse_datatype_string
from pyspark.sql.functions import from_json

assert json_schema ="""
    movie STRING
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

movie_silver = bronzeAugmentedDF.select("col", "nested_json.*")

# COMMAND ----------


