from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == '__main__':

    spark = (SparkSession
             .builder
             .appName("teste_gcs")
             .config("spark.jars", "/opt/spark/jars/gcs-connector-hadoop3-latest.jar")
             #.config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:1.9.4-hadoop3")
             .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
             .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
             .config("fs.gs.auth.service.account.enable", "true")
             .config("fs.gs.auth.service.account.json.keyfile", "/etc/gcp/sa_credentials.json")
             .enableHiveSupport()
             .getOrCreate()
             )

    data = [("James", "", "Smith", "36636", "M", 3000),
             ("Michael", "Rose", "", "40288", "M", 4000),
             ("Robert", "", "Williams", "42114", "M", 4000),
             ("Maria", "Anne", "Jones", "39192", "F", 4000),
             ("Jen", "Mary", "Brown", "", "F", -1)
             ]

    schema = StructType([
        StructField("firstname", StringType(), True),
        StructField("middlename", StringType(), True),
        StructField("lastname", StringType(), True),
        StructField("id", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("salary", IntegerType(), True)
    ])

    df = spark.createDataFrame(data=data, schema=schema)
    df.printSchema()
    df.show(truncate=False)

    (
        df
        .write
        .format("parquet")
        .mode("overwrite")
        .save('gs://poc_delta/poc_delta/teste')
    )

    spark.stop()
