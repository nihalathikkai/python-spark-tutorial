from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Learn").master("local[1]").getOrCreate()
    
    df1 = spark.read \
        .option("inferSchema", "true") \
        .option("header", True) \
        .option("sep", '|') \
        .csv("in/test.csv")
    
    df1.show()