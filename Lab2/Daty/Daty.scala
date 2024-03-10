// Databricks notebook source
// MAGIC %md
// MAGIC Użyj każdą z tych funkcji 
// MAGIC * `unix_timestamp()` 
// MAGIC * `date_format()`
// MAGIC * `to_unix_timestamp()`
// MAGIC * `from_unixtime()`
// MAGIC * `to_date()` 
// MAGIC * `to_timestamp()` 
// MAGIC * `from_utc_timestamp()` 
// MAGIC * `to_utc_timestamp()`

// COMMAND ----------

import org.apache.spark.sql.functions._

val kolumny = Seq("timestamp","unix", "Date")
val dane = Seq(("2015-03-22T14:13:34", 1646641525847L,"May, 2021"),
               ("2015-03-22T15:03:18", 1646641557555L,"Mar, 2021"),
               ("2015-03-22T14:38:39", 1646641578622L,"Jan, 2021"))

var dataFrame = spark.createDataFrame(dane).toDF(kolumny:_*)
  .withColumn("current_date",current_date().as("current_date"))
  .withColumn("current_timestamp",current_timestamp().as("current_timestamp"))
display(dataFrame)

// COMMAND ----------

dataFrame.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC ## unix_timestamp(..) & cast(..)

// COMMAND ----------

// MAGIC %md
// MAGIC Konwersja **string** to a **timestamp**.
// MAGIC
// MAGIC Lokalizacja funkcji 
// MAGIC * `pyspark.sql.functions` in the case of Python
// MAGIC * `org.apache.spark.sql.functions` in the case of Scala & Java

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1. Zmiana formatu wartości timestamp yyyy-MM-dd'T'HH:mm:ss 
// MAGIC `unix_timestamp(..)`
// MAGIC
// MAGIC Dokumentacja API `unix_timestamp(..)`:
// MAGIC > Convert time string with given pattern (see <a href="http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html" target="_blank">SimpleDateFormat</a>) to Unix time stamp (in seconds), return null if fail.
// MAGIC
// MAGIC `SimpleDataFormat` is part of the Java API and provides support for parsing and formatting date and time values.

// COMMAND ----------

dataFrame.select(unix_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC 2. Zmień format zgodnie z klasą `SimpleDateFormat`**yyyy-MM-dd HH:mm:ss**
// MAGIC   * a. Wyświetl schemat i dane żeby sprawdzicz czy wartości się zmieniły

// COMMAND ----------


val zmianaFormatu = dataFrame.withColumn("formatted_timestamp", date_format($"timestamp", "yyyy-MM-dd HH:mm:ss"))

zmianaFormatu.printSchema()
display(zmianaFormatu)

// COMMAND ----------

//unix_timestamp
val tempE = zmianaFormatu.select(unix_timestamp($"formatted_timestamp", "yyyy-MM-dd HH:mm:ss"))
display(tempE)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Stwórz nowe kolumny do DataFrame z wartościami year(..), month(..), dayofyear(..)

// COMMAND ----------

//date_format
val yearDate = dataFrame.withColumn("year", date_format($"timestamp", "yyyy"))
                        .withColumn("month", date_format($"timestamp", "MM"))
                        .withColumn("dayofyear", date_format($"timestamp", "D"))
display(yearDate)

// COMMAND ----------

//to_date()
val toDate = yearDate.select(to_date($"timestamp").as("DateFromString"))
display(toDate)

// COMMAND ----------

//from_unixtime()
val fromUnix = yearDate.select(from_unixtime($"unix").alias("FromUnixToString"))
display(fromUnix)

// COMMAND ----------

//to_timestamp()  
val toTimestamp  = dataFrame.withColumn("date_plus_2_hours", expr("to_timestamp(current_date) + interval 2 hours"))
display(toTimestamp)

// COMMAND ----------

//to_utc_timestamp()
val toUtcTimestamp = dataFrame.withColumn("date_plus_2_hours", expr("to_utc_timestamp(current_date, 'PST') + interval 2 hours"))
display(toUtcTimestamp)

// COMMAND ----------

//from_utc_timestamp()
val fromUtcTimestamp = toUtcTimestamp.select(from_utc_timestamp($"date_plus_2_hours", "JST").alias("ChangedTimeZone"))
display(fromUtcTimestamp)
