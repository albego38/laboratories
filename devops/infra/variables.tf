# Región donde se desplegarán los recursos
variable "location" {
  type    = string
  default = "northeurope"
}

# Grupo de recursos para entorno PRO
variable "resource_group_name_pro" {
  type = string
}

# Nombre del workspace Databricks para PRO
variable "databricks_name_pro" {
  type = string
}

# Nombre del Storage Account para PRO (ADLS Gen2)
variable "storage_account_name_pro" {
  type = string
}

# Grupo de recursos para entorno DEV
variable "resource_group_name_dev" {
  type = string
}

# Nombre del workspace Databricks para DEV
variable "databricks_name_dev" {
  type = string
}

# Nombre del Storage Account para DEV (ADLS Gen2)
variable "storage_account_name_dev" {
  type = string
}
