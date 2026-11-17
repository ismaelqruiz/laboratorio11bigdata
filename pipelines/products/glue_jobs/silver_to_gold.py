import sys
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
# ---------------------------------------------------------
# 1. Inicialización del trabajo AWS Glue
# ---------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)
# ---------------------------------------------------------
# 2. Configuración del Data Lake y almacenamiento S3
# ---------------------------------------------------------

SILVER_TABLE = "lakehouse.silver.products"
GOLD_TABLE = "lakehouse.gold.product_category_summary"

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3a.endpoint", "http://floci:4566")
hadoop_conf.set("fs.s3a.access.key", "test")
hadoop_conf.set("fs.s3a.secret.key", "test")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
hadoop_conf.set(
    "fs.s3a.impl",
    "org.apache.hadoop.fs.s3a.S3AFileSystem"
)
hadoop_conf.set(
    "fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)
# ---------------------------------------------------------
# 3. Lectura de la tabla Iceberg Silver
# ---------------------------------------------------------
print("Leyendo productos desde Iceberg Silver...")
silver_df = spark.table(SILVER_TABLE)
print(f"Registros leídos desde Silver: {silver_df.count()}")
# ---------------------------------------------------------
# 4. Agregación de indicadores por categoría
# ---------------------------------------------------------

print("Calculando indicadores por categoría...")

gold_df = (
    silver_df
    .groupBy("category")
    .agg(
        F.count("product_id").alias("product_count"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.sum("stock").alias("total_stock"),
        F.min("price").alias("min_price"),
        F.max("price").alias("max_price")
    )
    .withColumn("generated_at", F.current_timestamp())
)
total_categories = gold_df.count()
print(f"Categorías procesadas: {total_categories}")
# ---------------------------------------------------------
# 5. Crear el namespace Gold
# ---------------------------------------------------------
print("Preparando el namespace Iceberg Gold...")

spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS lakehouse.gold
""")
# ---------------------------------------------------------
# 6. Guardar los resultados en Iceberg Gold
# ---------------------------------------------------------

print("Guardando resultados en Iceberg Gold...")

(
    gold_df.writeTo(GOLD_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .createOrReplace()
)
print("Escritura de la tabla Gold finalizada.")
# ---------------------------------------------------------
# 7. Verificación de la tabla Gold
# --------------------------------------------------------
print("Verificando la tabla Iceberg Gold...")

result_df = spark.table(GOLD_TABLE)

print(f"Total de categorías en Gold: {result_df.count()}")

print("Esquema de la tabla Gold:")
result_df.printSchema()

print("Indicadores por categoría:")
result_df.orderBy("category").show(50, truncate=False)


# ---------------------------------------------------------
# 8. Finalización del trabajo
# ---------------------------------------------------------

job.commit()

print("Trabajo Silver a Gold finalizado correctamente.")
