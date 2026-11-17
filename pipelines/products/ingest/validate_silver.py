
import boto3

from botocore.config import Config
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ---------------------------------------------------------
# 1. Inicializar Spark con soporte para Apache Iceberg
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("validate-silver")
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
# 2. Configurar la conexión S3A con Floci
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
# 3. Leer la tabla Iceberg Silver
# ---------------------------------------------------------

print("\n========== VALIDACIÓN DE SILVER ==========")

silver_df = spark.table("lakehouse.silver.products")

total_products = silver_df.count()

print(f"Total de productos en Silver: {total_products}")

# ---------------------------------------------------------
# 4. Validación 1: identificadores y precios válidos
# ---------------------------------------------------------

invalid_products = silver_df.filter(
    F.col("product_id").isNull()
    | F.col("price").isNull()
    | (F.col("price") <= 0)
)

invalid_count = invalid_products.count()

print("\n--- Validación 1: integridad de datos ---")
print(f"Productos inválidos: {invalid_count}")

if invalid_count == 0:
    print("RESULTADO: CORRECTO")
    print("Todos los productos tienen product_id y price válidos.")
else:
    print("RESULTADO: REVISAR")
    invalid_products.show(10, truncate=False)

# ---------------------------------------------------------
# 5. Validación 2: identificadores duplicados
# ---------------------------------------------------------

duplicate_products = (
    silver_df
    .groupBy("product_id")
    .count()
    .filter(F.col("count") > 1)
)

duplicate_count = duplicate_products.count()

print("\n--- Validación 2: productos duplicados ---")
print(f"Identificadores duplicados: {duplicate_count}")

if duplicate_count == 0:
    print("RESULTADO: CORRECTO")
    print("No existen product_id duplicados.")
else:
    print("RESULTADO: REVISAR")
    duplicate_products.show(10, truncate=False)

# ---------------------------------------------------------
# 6. Resumen final
# ---------------------------------------------------------

print("\n========== RESUMEN FINAL ==========")
print(f"Total de productos: {total_products}")
print(f"Productos inválidos: {invalid_count}")
print(f"Identificadores duplicados: {duplicate_count}")

if invalid_count == 0 and duplicate_count == 0:
    print("VALIDACIÓN DE SILVER COMPLETADA CORRECTAMENTE.")
else:
    raise ValueError("Silver presenta errores de calidad de datos.")

spark.stop()
