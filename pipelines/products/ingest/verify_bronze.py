
import boto3
from botocore.config import Config

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path"})
)
response = s3.list_objects_v2(
    Bucket="ucv-data-platform",
    Prefix="products/bronze/"
)

objects = response.get("Contents", [])

print("VERIFICACIÓN DE LA CAPA BRONZE")
print(f"Archivos encontrados: {len(objects)}")

for obj in objects:
    print(f"Archivo: {obj['Key']}")
    print(f"Tamaño: {obj['Size']} bytes")
