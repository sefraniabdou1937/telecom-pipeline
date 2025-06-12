from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Création de la session Spark
spark = SparkSession.builder \
    .appName("TelecomBilling") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
    .getOrCreate()

# Configuration JDBC PostgreSQL
jdbc_url = "jdbc:postgresql://localhost:5432/telecom_rating"
properties = {
    "user": "postgres",
    "password": "0000",
    "driver": "org.postgresql.Driver"
}

# Chargement de la table de rating agrégé
monthly_usage_df = spark.read.jdbc(url=jdbc_url, table="monthly_rated_usage", properties=properties)

# Agrégation pour la facturation : total par client et par mois
bills_df = monthly_usage_df.groupBy("customer_id", "year_month").agg(
    F.sum("cost").alias("total_amount"),
    F.countDistinct("record_type").alias("num_services"),
    F.first("rate_plan_id").alias("rate_plan_id")  # facultatif mais utile pour la facture
)

# Affichage
bills_df.show(truncate=False)
bills_df.write.jdbc(
    url=jdbc_url,
    table="monthly_bills",
    mode="overwrite",  # ou "append" si tu veux cumuler
    properties=properties
)