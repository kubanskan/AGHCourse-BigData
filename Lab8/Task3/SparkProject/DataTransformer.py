from pyspark.sql.functions import col, when
class DataTransformer:
    def __init__(self, spark_session):
        self.spark = spark_session

    def categorize_age(self, df):
       new_df = df.withColumn('child_adult', when(col('age') < 18, 'Child').otherwise('Adult'))
       return new_df
