provider "azurerm" {
  features {}
}

#########################################
#               PRO                    #
#########################################

resource "azurerm_resource_group" "rg_pro" {
  name     = var.resource_group_name_pro
  location = var.location
}

resource "azurerm_databricks_workspace" "workspace_pro" {
  name                = var.databricks_name_pro
  location            = var.location
  resource_group_name = azurerm_resource_group.rg_pro.name
  sku                 = "trial"
  public_network_access_enabled = true

  custom_parameters {
    no_public_ip = "false"  # Permitir IP pública (SCC deshabilitado)
  }
}

resource "azurerm_storage_account" "storage_pro" {
  name                     = var.storage_account_name_pro
  resource_group_name      = azurerm_resource_group.rg_pro.name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"

  is_hns_enabled = true  # Habilita Data Lake Gen2
}


#########################################
#               DEV                    #
#########################################

resource "azurerm_resource_group" "rg_dev" {
  name     = var.resource_group_name_dev
  location = var.location
}

resource "azurerm_databricks_workspace" "workspace_dev" {
  name                = var.databricks_name_dev
  location            = var.location
  resource_group_name = azurerm_resource_group.rg_dev.name
  sku                 = "trial"
  public_network_access_enabled = true

  custom_parameters {
    no_public_ip = "false"
  }
}

resource "azurerm_storage_account" "storage_dev" {
  name                     = var.storage_account_name_dev
  resource_group_name      = azurerm_resource_group.rg_dev.name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"

  is_hns_enabled = true
}
