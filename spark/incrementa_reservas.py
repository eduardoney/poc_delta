from pyspark.sql import SparkSession
from pyspark.sql.types import DataType, StringType, StructType, StructField, TimestampType
from pyspark.sql.functions import from_json, col, to_date, to_timestamp
from delta.tables import *


if __name__ == "__main__":

    # Configurações Kafka
    broker = "34.135.189.150:9094"
    topic = "src-app-reservas"
    strategy = "earliest"

    # Criação aplicação spark
    spark = (
        SparkSession
        .builder
        .appName("incrementa_reservas_delta")
        .config("spark.jars", "/opt/spark/jars/gcs-connector-hadoop3-latest.jar")
        # .config("spark.jars", "./spark/jars/spark-sql-kafka-0-10_2.12-3.1.2.jar")
        #com.google.cloud.bigdataoss:gcs-connector:1.9.4-hadoop3
        .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,io.delta:delta-contribs_2.12:1.0.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.delta.logStore.gs.impl", "io.delta.storage.GCSLogStore")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("fs.gs.auth.service.account.enable", "true")
        .config("fs.gs.auth.service.account.json.keyfile", "/etc/gcp/sa_credentials.json")
        .enableHiveSupport()
        .config("spark.driver.memory", "8g")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    
    # Leitura dados Kafka
    df = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", topic)
        .option("startingOffsets", strategy)
        .option("includeHeaders", "true")
        .load()
    )

    schema = StructType([StructField("cod_reserva", StringType(), False),
                         StructField("agencia", StringType(), False),
                         StructField("dt_retirada", StringType(), False),
                         StructField("cod_grupo", StringType(), False),
                         StructField("dt_geracao", StringType(), False),
                         StructField("dt_devolucao", StringType(), False)
                         ])

    df = (
        df
        .select(from_json(col("value").cast('string'), schema).alias("data"))
        .select("data.cod_reserva",
                "data.agencia",
                to_date(col("data.dt_retirada"),
                        "yyyy-MM-dd").alias("dt_retirada"),
                "data.cod_grupo", to_timestamp(
                    col("data.dt_geracao"), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("dt_geracao_f"),
                to_date(col("data.dt_devolucao"),
                        "yyyy-MM-dd").alias("dt_devolucao"),
                "data.dt_geracao"
                )
          )

    #Tabela de destino
    deltaTable = DeltaTable.forPath(spark, "gs://poc_delta/stage/")
    
    # Function to upsert microBatchOutputDF into Delta table using merge
    def upsertToDelta(microBatchOutputDF, batchId):
        deltaTable.alias("t").merge(
            microBatchOutputDF.alias("s"),
            "s.cod_reserva = t.cod_reserva") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

    query = (
        df.writeStream
        .outputMode("update")
        .format("delta")
        .foreachBatch(upsertToDelta)
        .option("checkpointLocation", "/mnt/d/EngDados/poc_delta/stage/_checkpoints/incrementa-reservas-json")
        # .start("/mnt/d/EngDados/poc_delta/stage/")
        #.option("checkpointLocation", "gs://poc_delta/poc_delta/stage/_checkpoints/incrementa-reservas-json")
        .start()
    )

    query.awaitTermination()
    spark.stop()

    # .option("checkpointLocation", "path/to/HDFS/dir") \

    """ Append
        query = (
        df.writeStream
        .outputMode("append")
        .format("delta")
        .trigger(once=True)
        .option("checkpointLocation", "/mnt/d/EngDados/poc_delta/stage/_checkpoints/incrementa-reservas-json")
        # .start("/mnt/d/EngDados/poc_delta/stage/")
        #.option("checkpointLocation", "gs://poc_delta/poc_delta/stage/_checkpoints/incrementa-reservas-json")
        .start("gs://poc_delta/stage/")
    )
    """

