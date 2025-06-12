from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, when, to_timestamp
from pyspark.sql.types import *

# 1. Création de la SparkSession (sans .config("spark.jars.packages") ici car géré par spark-submit)
spark = SparkSession.builder \
    .appName("KafkaToPostgres_NormalizedCDR") \
    .getOrCreate()

# 2. Schéma des données attendues dans le flux Kafka (adapté aux clés fournies)
schema = StructType([
    StructField("record_type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("cell_id", StringType(), True),
    StructField("technology", StringType(), True),
    StructField("msisdn", StringType(), True),       # ID principal (sender_id, caller_id, user_id)
    StructField("peer_id", StringType(), True),      # receiver_id / callee_id
    StructField("duration", LongType(), True),
    StructField("volume_mb", DoubleType(), True),    # DATA seulement
    StructField("status", StringType(), True)
])

# 3. Lecture du flux Kafka
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "valid-cdr") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# 4. Extraction JSON et application du schéma
cdr_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# 5. Construction du customer_id selon record_type (en ajoutant le + devant le numéro si Maroc)
cdr_with_customer = cdr_df.withColumn(
    "customer_id",
    when(col("msisdn").rlike("^212\\d{9}$"), expr("concat('+', msisdn)"))
)

# 6. Construction des colonnes receiver_id (sms -> peer_id, voice -> peer_id, data -> NULL)
cdr_with_receiver = cdr_with_customer.withColumn(
    "receiver_id",
    when(col("record_type") == "sms", col("peer_id"))
    .when(col("record_type") == "voice", col("peer_id"))
    .otherwise(None)
)

# 7. Construction des colonnes callee_id (voice -> peer_id, sinon NULL)
cdr_with_callee = cdr_with_receiver.withColumn(
    "callee_id",
    when(col("record_type") == "voice", col("peer_id"))
    .otherwise(None)
)

# 8. Renommage et normalisation des colonnes duration et volume
cdr_normalized = cdr_with_callee \
    .withColumn("duration_sec", when(col("duration").isNull(), 0).otherwise(col("duration"))) \
    .withColumn("data_volume_mb", when(col("volume_mb").isNull(), 0.0).otherwise(col("volume_mb"))) \
    .withColumn("session_duration_sec", expr("0")) \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .filter((col("status") == "valid") & col("customer_id").isNotNull())

# 9. Sélection finale dans l'ordre de la table PostgreSQL
cdr_final_ordered = cdr_normalized.select(
    col("record_type").cast("string"),
    col("customer_id").cast("string"),
    col("cell_id").cast("string"),
    col("technology").cast("string"),
    col("receiver_id").cast("string"),
    col("callee_id").cast("string"),
    col("duration_sec").cast("long"),
    col("data_volume_mb").cast("double"),
    col("session_duration_sec").cast("long"),
    col("timestamp").cast("timestamp")
)

# 10. Fonction d'écriture par batch dans PostgreSQL
def write_to_postgres(batch_df, batch_id):
    count = batch_df.count()
    if count > 0:
        print(f"Batch {batch_id} : {count} enregistrements valides envoyés à PostgreSQL.")
        batch_df.show(truncate=False)
        try:
            batch_df.write \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://localhost:5432/telecom_rating") \
                .option("dbtable", "cdr_records_normalized") \
                .option("user", "postgres") \
                .option("password", "0000") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
        except Exception as e:
            print(f"Échec de l'écriture batch {batch_id} : {e}")

# 11. Démarrage de l'écriture en streaming avec checkpoint
query = cdr_final_ordered.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark-checkpoints/cdr_normalized") \
    .start()

query.awaitTermination()