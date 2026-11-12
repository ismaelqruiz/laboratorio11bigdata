
# Bucket principal del Data Lake
resource "aws_s3_bucket" "data_lake" {
  bucket = var.data_lake_bucket

  tags = {
    Project     = "UCV Big Data"
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

# Generar combinaciones entre dominios y capas
locals {
  domain_layer_pairs = flatten([
    for domain in var.domains : [
      for layer in var.layers : {
        domain = domain
        layer  = layer
        key    = "${domain}/${layer}/"
      }
    ]
  ])

  domain_layer_map = {
    for item in local.domain_layer_pairs :
    "${item.domain}-${item.layer}" => item
  }
}

# Crear los prefijos dentro del bucket S3
resource "aws_s3_object" "layer_prefixes" {
  for_each = local.domain_layer_map

  bucket  = aws_s3_bucket.data_lake.id
  key     = each.value.key
  content = ""
}

# Crear bases de datos Glue por dominio
resource "aws_glue_catalog_database" "domains" {
  for_each = var.domains

  catalog_id  = var.catalog_id
  name        = "${each.value}_domain"
  description = "Data Catalog for ${each.value} domain"

  tags = {
    Domain      = each.value
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
    lifecycle {
    ignore_changes = [tags]
  }
}
