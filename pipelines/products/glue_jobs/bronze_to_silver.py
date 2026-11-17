
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

# Crear el contexto de Spark y AWS Glue.
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

# Inicializar el trabajo.
job = Job(glue_context)
job.init(args["JOB_NAME"], args)


# ---------------------------------------------------------
# 2. Configuración del Data Lake y almacenamiento S3
# ---------------------------------------------------------

BUCKET = "ucv-data-platform"

BRONZE_PATH = f"s3a://{BUCKET}/products/bronze/"
SILVER_TABLE = "lakehouse.silver.products"

# Configuración de conexión S3A con Floci.
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
# 3. Lectura de los archivos JSON de Bronze
# ---------------------------------------------------------

print("Leyendo datos desde Bronze...")

raw_df = (
    spark.read
    .option("multiLine", "true")
    .json(BRONZE_PATH)
)

# Extraer cada producto del arreglo JSON.
products_df = (
    raw_df
    .select(F.explode("products").alias("product"))
    .select("product.*")
)

total_bronze = products_df.count()

print(f"Registros leídos desde Bronze: {total_bronze}")


# ---------------------------------------------------------
# 4. Transformación y limpieza de datos
# ---------------------------------------------------------

print("Transformando y limpiando los productos...")

# Seleccionar y normalizar los campos necesarios.
silver_df = products_df.select(
    F.col("id").cast("long").alias("product_id"),
    F.trim(F.col("title")).alias("title"),
    F.lower(F.trim(F.col("category"))).alias("category"),
    F.col("price").cast("decimal(10,2)").alias("price"),
    F.col("rating").cast("double").alias("rating"),
    F.col("stock").cast("integer").alias("stock"),
    F.col("brand").alias("brand"),
    F.current_timestamp().alias("processed_at")
)

# Eliminar registros sin identificador o con precios inválidos.
silver_df = silver_df.filter(
    F.col("product_id").isNotNull()
    & F.col("price").isNotNull()
    & (F.col("price") > 0)
)

# Eliminar productos duplicados según su identificador.
silver_df = silver_df.dropDuplicates(["product_id"])

total_silver = silver_df.count()

print(f"Registros válidos en Silver: {total_silver}")


# ---------------------------------------------------------
# 5. Crear el namespace de Iceberg
# ---------------------------------------------------------

print("Preparando el namespace Iceberg...")

spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS lakehouse.silver
""")


# ---------------------------------------------------------
# 6. Guardar los datos en la tabla Iceberg Silver
# ---------------------------------------------------------

print("Guardando los datos en Iceberg Silver...")

(
    silver_df.writeTo(SILVER_TABLE)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .createOrReplace()
)

print("Escritura en Iceberg finalizada.")


# ---------------------------------------------------------
# 7. Verificación de la tabla Silver
# ---------------------------------------------------------

print("Verificando la tabla Iceberg Silver...")

result_df = spark.table(SILVER_TABLE)

total_result = result_df.count()

print("Tabla Silver creada correctamente.")
print(f"Total de productos Silver: {total_result}")

# Mostrar la estructura de la tabla.
print("Esquema de la tabla Silver:")
result_df.printSchema()

# Mostrar los primeros 10 productos.
print("Primeros 10 registros de Silver:")
result_df.show(10, truncate=False)


# ---------------------------------------------------------
# 8. Finalización del trabajo
# ---------------------------------------------------------

job.commit()

print("Trabajo Bronze a Silver finalizado correctamente.")
