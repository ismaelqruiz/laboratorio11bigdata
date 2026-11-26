
# ---------------------------------------------------------
# CONFIGURACIÓN GENERAL DE AWS Y TERRAFORM
# ---------------------------------------------------------

variable "aws_region" {
  description = "Region AWS simulada."
  type        = string
  default     = "us-east-1"
}

variable "data_lake_bucket" {
  description = "Nombre del bucket principal del Data Lake."
  type        = string
  default     = "ucv-data-platform"
}

variable "environment" {
  description = "Ambiente de trabajo."
  type        = string
  default     = "dev"
}

# ---------------------------------------------------------
# DOMINIOS Y CAPAS DE LA ARQUITECTURA MEDALLION
# ---------------------------------------------------------

variable "domains" {
  description = "Dominios de datos de la plataforma."
  type        = set(string)

  default = [
    "sales",
    "customers",
    "products",
    "marketing"
  ]
}

variable "layers" {
  description = "Capas de la arquitectura Medallion."
  type        = set(string)

  default = [
    "bronze",
    "silver",
    "gold"
  ]
}

# ---------------------------------------------------------
# CONFIGURACIÓN DEL GLUE DATA CATALOG
# ---------------------------------------------------------

variable "catalog_id" {
  description = "Catalog ID simulado para Glue."
  type        = string
  default     = "000000000000"
}

# ---------------------------------------------------------
# CONFIGURACIÓN DE AWS GLUE JOBS - LABORATORIO 13
# ---------------------------------------------------------

variable "glue_role_arn" {
  description = "ARN simulado del rol utilizado por Glue Jobs en Floci."
  type        = string
  default     = "arn:aws:iam::000000000000:role/ucv-glue-role"
}

variable "glue_job_run_timeout" {
  description = "Timeout declarativo del Glue Job, en minutos."
  type        = number
  default     = 30
}
