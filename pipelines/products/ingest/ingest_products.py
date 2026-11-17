
import json
import boto3
import requests

from datetime import datetime, timezone
from botocore.config import Config


# ---------------------------------------------------------
# 1. Configuración de la API y del almacenamiento S3
# ---------------------------------------------------------

API_URL = "https://dummyjson.com/products?limit=0"

BUCKET_NAME = "ucv-data-platform"
S3_ENDPOINT = "http://localhost:4566"

AWS_ACCESS_KEY = "test"
AWS_SECRET_KEY = "test"
AWS_REGION = "us-east-1"


# ---------------------------------------------------------
# 2. Consumir la API REST de DummyJSON
# ---------------------------------------------------------

print("Consultando la API de productos...")

response = requests.get(API_URL, timeout=30)

# Verificar que la API responda correctamente.
response.raise_for_status()

data = response.json()

products = data.get("products", [])

if not products:
    raise ValueError("La API no devolvió productos.")

print(f"HTTP Status: {response.status_code}")
print(f"Productos obtenidos: {len(products)}")


# ---------------------------------------------------------
# 3. Generar la ruta de almacenamiento en Bronze
# ---------------------------------------------------------

now = datetime.now(timezone.utc)

ingestion_date = now.strftime("%Y-%m-%d")
timestamp = now.strftime("%Y%m%dT%H%M%SZ")

s3_key = (
    f"products/bronze/"
    f"ingestion_date={ingestion_date}/"
    f"products_{timestamp}.json"
)


# ---------------------------------------------------------
# 4. Crear cliente S3 conectado a Floci
# ---------------------------------------------------------

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION,
    config=Config(
        s3={"addressing_style": "path"}
    )
)


# ---------------------------------------------------------
# 5. Guardar la respuesta original en Bronze
# ---------------------------------------------------------

raw_json = json.dumps(data, ensure_ascii=False, indent=2)

s3.put_object(
    Bucket=BUCKET_NAME,
    Key=s3_key,
    Body=raw_json.encode("utf-8"),
    ContentType="application/json"
)

print("\nIngesta finalizada correctamente.")
print(f"Bucket: {BUCKET_NAME}")
print(f"Ruta Bronze: s3://{BUCKET_NAME}/{s3_key}")
print(f"Total de productos almacenados: {len(products)}")
