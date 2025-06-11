from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# === 1. Initialiser Spark avec support Kafka ===
spark = SparkSession.builder \
    .appName("CDR Mediation") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# === 2. Schéma brut JSON attendu depuis Kafka ===
raw_schema = StructType([
    StructField("record_type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("caller_id", StringType(), True),
    StructField("callee_id", StringType(), True),
    StructField("sender_id", StringType(), True),
    StructField("receiver_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("duration_sec", IntegerType(), True),
    StructField("data_volume_mb", DoubleType(), True),
    StructField("session_duration_sec", IntegerType(), True),
    StructField("cell_id", StringType(), True),
    StructField("technology", StringType(), True)
])

# === 3. Lecture des CDRs bruts depuis Kafka ===
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "raw-cdr") \
    .option("startingOffsets", "latest") \
    .load()

# === 4. Parsing du JSON ===
json_df = raw_df.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("data", from_json(col("json_str"), raw_schema)) \
    .select("data.*")

# === 5. Normalisation des données ===
normalized_df = json_df.withColumn("msisdn",
                    coalesce(col("caller_id"), col("sender_id"), col("user_id"))) \
    .withColumn("peer_id",
                    coalesce(col("callee_id"), col("receiver_id"))) \
    .withColumn("volume_mb", col("data_volume_mb")) \
    .withColumn("duration", coalesce(col("duration_sec"), col("session_duration_sec"))) \
    .withColumn("status", lit("valid")) \
    .drop("caller_id", "callee_id", "sender_id", "receiver_id", "user_id", "data_volume_mb", "duration_sec", "session_duration_sec")

# === 6. Validation des enregistrements ===
validated_df = normalized_df.withColumn("is_valid",
    when(col("msisdn").isNull(), False)
    .when(col("msisdn").rlike("^999"), False)
    .when(col("record_type").isNull(), False)
    .otherwise(True)
)

# === 7. Séparation des enregistrements valides / invalides ===
valid_df = validated_df.filter(col("is_valid") == True).drop("is_valid")
invalid_df = validated_df.filter(col("is_valid") == False) \
    .withColumn("status", lit("error")) \
    .drop("is_valid")

# === 8. Format Kafka (clé/valeur) ===
valid_to_kafka = valid_df.selectExpr("CAST(null AS STRING) as key", "to_json(struct(*)) AS value")
invalid_to_kafka = invalid_df.selectExpr("CAST(null AS STRING) as key", "to_json(struct(*)) AS value")

# === 9. Écriture vers Kafka ===
valid_query = valid_to_kafka.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "valid-cdr") \
    .option("checkpointLocation", "file:///C:/kafka/checkpoints/valid") \
    .start()

invalid_query = invalid_to_kafka.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "invalid-cdr") \
    .option("checkpointLocation", "file:///C:/kafka/checkpoints/invalid") \
    .start()

# === 10. Démarrage du stream ===
spark.streams.awaitAnyTermination()
