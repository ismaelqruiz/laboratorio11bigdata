
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ---------------------------------------------------------
# 1. Inicializar Spark con Apache Iceberg
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("validate-gold")
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
# 2. Configurar conexión S3A con Floci
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
# 3. Leer nuevamente la tabla Iceberg Gold
# ---------------------------------------------------------

print("\n========== VALIDACIÓN DE GOLD ==========")

gold_df = spark.table(
    "lakehouse.gold.product_category_summary"
)

total_rows = gold_df.count()

print(f"Registros leídos desde Gold: {total_rows}")


# ---------------------------------------------------------
# 4. Captura 6: una fila por categoría
# ---------------------------------------------------------

print("\n--- VALIDACIÓN 1: UNA FILA POR CATEGORÍA ---")

duplicate_categories = (
    gold_df
    .groupBy("category")
    .count()
    .filter(F.col("count") > 1)
)

duplicate_category_count = duplicate_categories.count()

print(f"Categorías duplicadas: {duplicate_category_count}")

if duplicate_category_count == 0:
    print("RESULTADO: CORRECTO")
    print("Existe una sola fila por cada categoría.")
else:
    print("RESULTADO: REVISAR")
    duplicate_categories.show(truncate=False)


# ---------------------------------------------------------
# 5. Captura 7: product_count mayor que cero
# ---------------------------------------------------------

print("\n--- VALIDACIÓN 2: PRODUCT_COUNT > 0 ---")

invalid_categories = gold_df.filter(
    F.col("product_count").isNull()
    | (F.col("product_count") <= 0)
)

invalid_category_count = invalid_categories.count()

print(f"Categorías con product_count inválido: {invalid_category_count}")

if invalid_category_count == 0:
    print("RESULTADO: CORRECTO")
    print("Todas las categorías tienen product_count > 0.")
else:
    print("RESULTADO: REVISAR")
    invalid_categories.show(truncate=False)


# ---------------------------------------------------------
# 6. Resumen y verificación de lectura Iceberg
# ---------------------------------------------------------

print("\n========== RESUMEN FINAL ==========")
print(f"Filas en Gold: {total_rows}")
print(f"Categorías duplicadas: {duplicate_category_count}")
print(f"Categorías con product_count inválido: {invalid_category_count}")

if duplicate_category_count == 0 and invalid_category_count == 0:
    print("VALIDACIÓN DE GOLD COMPLETADA CORRECTAMENTE.")
else:
    raise ValueError("La tabla Gold presenta errores de validación.")

spark.stop()
