from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Création de la session Spark
spark = SparkSession.builder \
    .appName("TelecomRating") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
    .getOrCreate()

# Configuration JDBC PostgreSQL
jdbc_url = "jdbc:postgresql://localhost:5432/telecom_rating"
properties = {
    "user": "postgres",
    "password": "0000",
    "driver": "org.postgresql.Driver"
}

# Chargement des tables PostgreSQL
tables = ["cdr_records_normalized", "customers", "product_rates", "products", "rate_plans"]

dataframes = {}
for table in tables:
    df = spark.read.jdbc(url=jdbc_url, table=table, properties=properties)
    dataframes[table] = df
    print(f"[OK] Table '{table}' chargée avec {df.count()} lignes.")

# Fonction de tarification mensuelle
def perform_rating_agg(dataframes):
    cdr_df = dataframes['cdr_records_normalized']
    customers_df = dataframes['customers']
    product_rates_df = dataframes['product_rates']

    active_customers = customers_df.filter(F.col("status") == "active").select("customer_id", "rate_plan_id")
    cdr_df = cdr_df.withColumn("year_month", F.date_format("timestamp", "yyyy-MM"))
    cdr_with_plan = cdr_df.join(active_customers, "customer_id", "inner")

    cdr_with_plan = cdr_with_plan.withColumn(
        "consumption_units",
        F.when(F.col("record_type") == "voice", F.col("duration_sec"))
         .when(F.col("record_type") == "sms", F.lit(1))
         .when(F.col("record_type") == "data", F.col("data_volume_mb"))
         .otherwise(F.lit(0))
    )

    consumption_monthly = cdr_with_plan.groupBy(
        "customer_id", "rate_plan_id", "record_type", "year_month"
    ).agg(
        F.sum("consumption_units").alias("total_consumption_units")
    )

    # Renommer la colonne rate_plan_id de product_rates_df pour éviter ambiguïté
    pr = product_rates_df.withColumnRenamed("rate_plan_id", "pr_rate_plan_id")

    rated_monthly = consumption_monthly.join(
        pr,
        (consumption_monthly.rate_plan_id == pr.pr_rate_plan_id) &
        (consumption_monthly.record_type == pr.service_type),
        "left"
    )

    rated_monthly = rated_monthly.withColumn(
        "billable_units",
        F.when(F.col("total_consumption_units") > F.col("free_units"),
               F.col("total_consumption_units") - F.col("free_units")).otherwise(0)
    )

    rated_monthly = rated_monthly.withColumn(
        "cost",
        F.col("billable_units") * F.col("unit_price")
    )

    result_df = rated_monthly.select(
        "customer_id",
        "rate_plan_id",   # celle de consumption_monthly
        "record_type",
        "year_month",
        "total_consumption_units",
        "free_units",
        "billable_units",
        "unit_price",
        "cost"
    )

    return result_df

# Exécution du moteur de tarification
rated_monthly_df = perform_rating_agg(dataframes)
rated_monthly_df.show(20)
from pyspark.sql.functions import lit

# Écriture dans PostgreSQL
rated_monthly_df.write.jdbc(
    url=jdbc_url,
    table="monthly_rated_usage",
    mode="append",  # changer en "overwrite" si tu veux réécrire toute la table
    properties=properties
)

# Fermeture de Spark
spark.stop()