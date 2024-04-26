class DataReader:
    def __init__(self, spark_session):
        self.spark = spark_session

    def read_csv(self, filepath):
        return self.spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(filepath)
