# Databricks notebook source
# MAGIC %md
# MAGIC ## Zadanie 2

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
schemat = StructType([
    StructField("imdb_title_id", StringType(), False),
    StructField("ordering", IntegerType(), False),
    StructField("imdb_name_id", StringType(), False),
    StructField("category", StringType(), False),
    StructField("job", StringType(), True),
    StructField("characters", StringType(), True)
])

# COMMAND ----------

filePath = "dbfs:/FileStore/tables/Files/actors.csv"
actorsDf = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schemat) \
    .load(filePath)

# COMMAND ----------

display(actorsDf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Zadanie 3

# COMMAND ----------

filePath = "dbfs:/FileStore/tables/Files/actors.json"
actorsJsonDf = spark.read.format("json") \
    .option("multiline", "true") \
    .schema(schemat) \
    .load(filePath)

# COMMAND ----------

display(actorsJsonDf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Zadanie 4 

# COMMAND ----------

data = '''id,name,age
1,Ania,30
2,Kasia,ten
Nan,Zosia,25
3,Zuzia,17'''

dbutils.fs.put("dbfs:/FileStore/tables/Files/example_data.csv", data, overwrite=True)


# COMMAND ----------

schemat = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("age", IntegerType(), False)
])


# COMMAND ----------

PERMISSIVE = spark.read.format("csv") \
  .option("header", "true") \
  .schema(schemat) \
  .option("mode", "PERMISSIVE") \
  .load("dbfs:/FileStore/tables/Files/example_data.csv") 

# COMMAND ----------

display(PERMISSIVE)

# COMMAND ----------

DROPMALFORMED = spark.read.format("csv") \
  .option("header", "true") \
  .schema(schemat) \
  .option("mode", "DROPMALFORMED") \
  .load("dbfs:/FileStore/tables/Files/example_data.csv") 

# COMMAND ----------

display(DROPMALFORMED)

# COMMAND ----------

FAILFAST = spark.read.format("csv") \
  .option("header", "true") \
  .schema(schemat) \
  .option("mode", "FAILFAST") \
  .load("dbfs:/FileStore/tables/Files/example_data.csv")

# COMMAND ----------

display(FAILFAST)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Zadanie 5

# COMMAND ----------

PERMISSIVE.write.format("parquet").mode("overwrite").save("/FileStore/tables/test.parquet")
test = spark.read.format("parquet").load("/FileStore/tables/test.parquet")
display(test)

# COMMAND ----------

PERMISSIVE.write.format("json").mode("overwrite").save("/FileStore/tables/test.json")
test2 = spark.read.format("json").load("/FileStore/tables/test.json")
display(test2)
