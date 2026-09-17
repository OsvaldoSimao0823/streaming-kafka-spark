"""
Consumer: lê continuamente os preços do topic Kafka 'crypto_prices',
agrega-os por janelas de 1 minuto (média do preço), e persiste
o resultado em ficheiros Parquet.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.sql.functions import from_json, col, window, avg, to_timestamp, date_format

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "crypto_prices"
KAFKA_CONNECTOR_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"

OUTPUT_PATH = "output/crypto_agregado"
CHECKPOINT_PATH = "output/checkpoint"

JANELA = "1 minute"
WATERMARK = "30 seconds"

schema = StructType([
    StructField("bitcoin_usd", DoubleType()),
    StructField("ethereum_usd", DoubleType()),
    StructField("timestamp", DoubleType())
])


def main():
    spark = SparkSession.builder \
        .appName("CryptoStreamConsumer") \
        .config("spark.jars.packages", KAFKA_CONNECTOR_PACKAGE) \
        .getOrCreate()

    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    df_json = df_kafka.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("dados")) \
        .select("dados.*") \
        .withColumn("event_time", to_timestamp(col("timestamp")))

    resultado = df_json \
        .withWatermark("event_time", WATERMARK) \
        .groupBy(window(col("event_time"), JANELA)) \
        .agg(
            avg("bitcoin_usd").alias("media_btc"),
            avg("ethereum_usd").alias("media_eth")
        ) \
        .select(
            date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss").alias("inicio_janela"),
            date_format(col("window.end"), "yyyy-MM-dd HH:mm:ss").alias("fim_janela"),
            col("media_btc"),
            col("media_eth")
        )

    query = resultado.writeStream \
        .format("parquet") \
        .option("path", OUTPUT_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .outputMode("append") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()