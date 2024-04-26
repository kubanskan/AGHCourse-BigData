class DataWriter:
    def __init__(self, spark_session):
        self.spark = spark_session

    def write_csv(self, df, filepath):
        return df.write.csv(filepath, header=True, mode='overwrite')
