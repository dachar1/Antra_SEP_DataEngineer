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

language_silver = get_language_table(movies_bronze)

# COMMAND ----------

genres_silver = get_genres_table(movies_bronze)
#display(genres_silver)

# COMMAND ----------


movies_silver = movies_silver.withColumn("movie_genre_junction_id", monotonically_increasing_id()+1)
movie_genre_junction_silver = get_movie_genre_junction_table(movies_silver)
# display(movie_genre_junction_silver)

# COMMAND ----------

movies_silver = movies_silver.join(language_silver, movies_silver.OriginalLanguage == language_silver.OriginalLanguage, "inner").drop("OriginalLanguage")
display(movies_silver)

# COMMAND ----------

movies_silver = movies_silver.drop("genres")

# COMMAND ----------

set_df_columns_nullable(spark, movies_silver,['Language_Id','movie_genre_junction_id'])

# COMMAND ----------

from pyspark.sql.types import _parse_datatype_string

assert movies_silver.schema == _parse_datatype_string(
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


movies_silver_clean = movies_silver.filter(movies_silver.RunTime >= 0)
movies_silver_quarantined = movies_silver.filter(movies_silver.RunTime < 0)

# COMMAND ----------


display(movies_silver_quarantined)

# COMMAND ----------

(
    movies_silver_clean.select(
        "BackdropUrl", "Budget", "CreatedBy", "CreatedDate",
        "Id", "ImdbUrl", "Overview", "PosterUrl", "Price", "ReleaseTime", "ReleaseDate",
        "ReleaseYear", "Revenue", "RunTime", "Tagline", "Title", "TmdbUrl", "UpdatedBy",
        "UpdatedDate", "movie_genre_junction_id", "Language_Id"
    )
    .write.format("delta")
    .mode("append")
    .partitionBy("ReleaseYear")
    .save(silverPath)
)


# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS movies_silver
"""
)

spark.sql(
    f"""
CREATE TABLE movies_silver
USING DELTA
LOCATION "{silverPath}"
"""
)

# COMMAND ----------


from pyspark.sql.types import _parse_datatype_string

silverTable = spark.read.table("movies_silver")
expected_schema = """
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

assert silverTable.schema == _parse_datatype_string(
    expected_schema
), "Schemas do not match"
print("Assertion passed.")

# COMMAND ----------

from delta.tables import DeltaTable

bronzeTable = DeltaTable.forPath(spark, bronzePath)
silverAugmented = movies_silver_clean.withColumn("status", lit("loaded"))

update_match = "bronze.movie = clean.movie"
update = {"status": "clean.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("clean"), update_match)
    .whenMatchedUpdate(set = update)
    .execute()
)


# COMMAND ----------


silverAugmented = movies_silver_quarantined.withColumn(
    "status", lit("quarantined")
)

update_match = "bronze.movie = quarantine.movie"
update = {"status": "quarantine.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("quarantine"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)


# COMMAND ----------


