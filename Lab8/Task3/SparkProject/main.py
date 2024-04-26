from pyspark.sql import SparkSession
from DataReader import DataReader
from DataTransformer import DataTransformer
from DataWriter import DataWriter

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("FirstSparkApp").getOrCreate()

    input_filepath = "Data/TitanicData-Modified.csv"
    output_filepath = "Data/After_transformationTitanicData-Modified.csv"

    reader = DataReader(spark)
    transformer = DataTransformer(spark)
    writer = DataWriter(spark)

    df = reader.read_csv(input_filepath)

    transformed_df = transformer.categorize_age(df)

    writer.write_csv(transformed_df, output_filepath)


