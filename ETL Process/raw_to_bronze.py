# Databricks notebook source
# MAGIC %run ./includes/configuration

# COMMAND ----------

dbutils.fs.rm(bronzePath, recurse=True)

# COMMAND ----------

from pyspark.sql.functions import explode, col, to_json
movie_raw = spark.read.json(path = f"/FileStore/movie/*", multiLine = True)
movie_raw = movie_raw.select("movie", explode("movie"))
movie_raw = movie_raw.drop(col("movie")).toDF('movie')

display(movie_raw)

# COMMAND ----------


from pyspark.sql.functions import current_timestamp, lit

movie_raw = movie_raw.select(
    "movie",
    lit("www.imdb.com").alias("datasource"),
    lit("new").alias("status"),
    current_timestamp().alias("ingesttime"),
    current_timestamp().cast("date").alias("ingestdate")
)

# COMMAND ----------

(
movie_raw.select("datasource", "ingesttime", "movie", "status", col("ingestdate").alias("p_ingestdate"))
    .write.format("delta")
    .mode("append")
    .partitionBy("p_ingestdate")
    .save(bronzePath)
)

# COMMAND ----------

movie_raw.printSchema()

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

spark.sql("""
DROP TABLE IF EXISTS movie_bronze;
""")

spark.sql(f"""
CREATE TABLE movie_bronze
USING DELTA
LOCATION "{bronzePath}"
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM movie_bronze 
# MAGIC LIMIT 5

# COMMAND ----------


movies_bronze = spark.read.load(path = bronzePath)

# COMMAND ----------


