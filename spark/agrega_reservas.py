from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import col, datediff, udf, explode, to_date
from delta.tables import *
import datetime


def dateDiffList(qtd: int, data_inicio: datetime.date):
    """Função que retorna uma lista de datas, utilizando data_inicio como referenncia até a quantidade de dias desejada

    Args:
        qtd (int): Quantidade de dias para geração da lista
        data_inicio (datetime.date): Data inicio de referência.

    Returns:
        List: Lista de datas no formato YYYY-MM-DD HH:MI:SS
    """

    dt_aux = data_inicio
    lista_data = []
    for i in range(qtd):
        if dt_aux == data_inicio:
            lista_data.append(data_inicio.strftime("%Y-%m-%d %H:%M:%S"))
        else:
            lista_data.append(dt_aux.strftime("%Y-%m-%d %H:%M:%S"))

        # Incrementa um dia na data.
        dt_aux = dt_aux + datetime.timedelta(days=1)
    return lista_data


if __name__ == "__main__":
    # Cria spark session
    spark = (
        SparkSession
        .builder
        .appName("agrega_reservas")
        .config("spark.jars", "/opt/spark/jars/gcs-connector-hadoop3-latest.jar")
        # .config("spark.jars", "./spark/jars/spark-sql-kafka-0-10_2.12-3.1.2.jar")
        # com.google.cloud.bigdataoss:gcs-connector:1.9.4-hadoop3
        .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0,io.delta:delta-contribs_2.12:1.0.0")
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

    # Leitura dos dados no storage
    df_reservas = (
        spark.read.format("delta").load("gs://poc_delta/stage/")
    )

    # Mostra o schema do arquivo.
    print("Schema inicial")
    print(df_reservas.printSchema())

    # Cria uma coluna para o numero de diárias
    df_reservas = df_reservas.withColumn(
        "qtd_diarias", datediff("dt_devolucao", "dt_retirada"))
    print(df_reservas.show(5))

    # Registra a UDF para utilização no Spark
    dateDiffListUdf = udf(f=lambda qtd, data_inicio: dateDiffList(
        qtd, data_inicio), returnType=ArrayType(StringType()))

    # Adiciona uma coluna ao dataframe com a lista de datas
    df_reservas = df_reservas.withColumn(
        "lista_datas", dateDiffListUdf(col("qtd_diarias"), col("dt_retirada")))

    # Retorna uma linha para cada data da lista, renomeando o campo para dt_referencia
    df_quebra = (df_reservas.select("cod_reserva", "agencia", "cod_grupo", explode("lista_datas"))
                 .withColumnRenamed("col", "dt_referencia")
                 .withColumn("dt_referencia", to_date("dt_referencia", "yyyy-MM-dd HH:mm:ss")))

    # Mosta o schema final
    print("Schema final")
    df_quebra.printSchema()

    # Cria uma view para facilitar na query de agregação
    df_quebra.createOrReplaceTempView("reserva_quebra")

    #Dados finais
    spark.sql("""
          select agencia, cod_grupo, dt_referencia, count(1) as qtd
          from reserva_quebra
          group by agencia, cod_grupo, dt_referencia
          order by agencia, cod_grupo, dt_referencia"""
              ).show()

    spark.stop()