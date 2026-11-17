
from pyspark.sql import SparkSession


# ---------------------------------------------------------
# 1. Inicialización de Spark con soporte Apache Iceberg
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("query-iceberg-tables")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config(
        "spark.sql.catalog.lakehouse",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config(
        "spark.sql.catalog.lakehouse.warehouse",
        "s3a://ucv-data-platform/products/"
    )
    .getOrCreate()
)


# ---------------------------------------------------------
# 2. Configuración de conexión S3A con Floci
# ---------------------------------------------------------

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3a.endpoint", "http://floci:4566")
hadoop_conf.set("fs.s3a.access.key", "test")
hadoop_conf.set("fs.s3a.secret.key", "test")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
hadoop_conf.set(
    "fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)


# ---------------------------------------------------------
# 3. Consulta de la tabla Iceberg Silver
# ---------------------------------------------------------

print("\n========== CONSULTA ICEBERG SILVER ==========")

silver_df = spark.table("lakehouse.silver.products")

total_silver = silver_df.count()

print(f"Registros disponibles en Silver: {total_silver}")

print("Primeros 5 productos de Silver:")
silver_df.show(5, truncate=False)


# ---------------------------------------------------------
# 4. Consulta de la tabla Iceberg Gold
# ---------------------------------------------------------

print("\n========== CONSULTA ICEBERG GOLD ==========")

gold_df = spark.table(
    "lakehouse.gold.product_category_summary"
)

total_gold = gold_df.count()

print(f"Registros disponibles en Gold: {total_gold}")

print("Indicadores por categoría:")
gold_df.orderBy("category").show(30, truncate=False)


# ---------------------------------------------------------
# 5. Resumen de la verificación
# ---------------------------------------------------------

print("\n========== VERIFICACIÓN FINAL ==========")

print(f"Registros Silver: {total_silver}")
print(f"Registros Gold: {total_gold}")

if total_silver > 0 and total_gold > 0:
    print("Las tablas Iceberg Silver y Gold pueden releerse.")
    print("VERIFICACIÓN ICEBERG COMPLETADA CORRECTAMENTE.")
else:
    raise ValueError("No se pudieron verificar las tablas Iceberg.")


# ---------------------------------------------------------
# 6. Cerrar Spark
# ---------------------------------------------------------

spark.stop()
