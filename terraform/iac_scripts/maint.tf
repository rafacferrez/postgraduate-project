#Define random
resource "random_string" "naming" {
  special = false
  upper   = false
  length  = 6
}

#Define locals
locals {
  prefix = "databricks${random_string.naming.result}"
  tags = {
    Environment = "Analytcs"
    Owner       = lookup(data.external.me.result, "name")
    Project = "final_project"
  }
}

#Create Resource Group
resource "azurerm_resource_group" "rg_databricks" {
  name     = "${local.prefix}-rg"
  location = var.region
  tags     = local.tags
}

#Create a App Registration to Databricks cluster
resource "azuread_application" "app_storage_databricks" {
  display_name = "app_storage_databricks"
  owners       = [data.azurerm_client_config.current.object_id]
}

#Create a Secret for App Registration
resource "azuread_application_password" "app_storage_databricks_secret" {
  application_object_id = azuread_application.app_storage_databricks.object_id
}

#Create a ServicePrincipal
resource "azuread_service_principal" "service_principal_databricks" {
  application_id               = azuread_application.app_storage_databricks.application_id
  app_role_assignment_required = false
}

#Create a Storage Account for blobs with Hierarchy namespace
resource "azurerm_storage_account" "sadatalake" {
  name                     = var.storage_datalake
  resource_group_name      = azurerm_resource_group.rg_databricks.name
  location                 = azurerm_resource_group.rg_databricks.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true
  tags = {
    environment = "Analytcs"
  }
}

#Create a Container called Bronze using azurerm_storage_container
resource "azurerm_storage_container" "containerbronze" {
  name                  = "bronze"
  storage_account_name  = azurerm_storage_account.sadatalake.name
  container_access_type = "private"
}

#Create a Container called Silver
resource "azurerm_storage_container" "containersilver" {
  name                  = "silver"
  storage_account_name  = azurerm_storage_account.sadatalake.name
  container_access_type = "private"
}

#Create a Container called Gold
resource "azurerm_storage_container" "containergold" {
  name                  = "gold"
  storage_account_name  = azurerm_storage_account.sadatalake.name
  container_access_type = "private"
}

# assign reader role for team working on subscription sub-dev
resource "azurerm_role_assignment" "app_databricks_contributor_role" {
  scope                = azurerm_storage_account.sadatalake.id
  role_definition_name = "Contributor"
  principal_id       = azuread_service_principal.service_principal_databricks.id
}

#Create a Keyvault with policies
resource "azurerm_key_vault" "azkeyvault" {
  name                       = "azkeyvault${random_string.naming.result}"
  location                   = azurerm_resource_group.rg_databricks.location
  resource_group_name        = azurerm_resource_group.rg_databricks.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  soft_delete_retention_days = 7

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id

    key_permissions = [
      "Create",
      "Get",
    ]

    secret_permissions = [
      "Set",
      "Get",
      "Delete",
      "Purge",
      "Recover",
      "List"
    ]
  }
  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = azuread_application.app_storage_databricks.application_id

    key_permissions = [
      "Create",
      "Get",
    ]

    secret_permissions = [
      "Set",
      "Get",
      "Delete",
      "Purge",
      "Recover",
      "List"
    ]
  }
}

#Create a secret
resource "azurerm_key_vault_secret" "secretappstoragedatabricks" {
  name         = "secret-app-storage-databricks"
  value        = azuread_application_password.app_storage_databricks_secret.value
  key_vault_id = azurerm_key_vault.azkeyvault.id
}

#Create a Databricks Workspace
resource "azurerm_databricks_workspace" "databricks_workspace" {
  name                        = "${local.prefix}-workspace"
  resource_group_name         = azurerm_resource_group.rg_databricks.name
  location                    = azurerm_resource_group.rg_databricks.location
  sku                         = "standard"
  managed_resource_group_name = "${local.prefix}-workspace-rg"
  tags                        = local.tags
}

#Create a Databricks Cluster using App Registration to connect Storage
resource "databricks_cluster" "dtb_cluster" {
  cluster_name            = var.cluster_dev_name
  spark_version           = var.databricks_spark_version
  node_type_id            = var.databricks_node_type
  autotermination_minutes = 20
  num_workers = 0
  spark_conf = {
    "spark.databricks.cluster.profile" : "singleNode"
    "spark.master" : "local[*]"
  }
  custom_tags = {
    "ResourceClass" = "SingleNode"
  }
  depends_on = [azurerm_databricks_workspace.databricks_workspace]
}

#upload csv to bronze container
resource "azurerm_storage_blob" "uploadcsv" {
  name                   = "addresses.csv"
  storage_account_name   = azurerm_storage_account.sadatalake.name
  storage_container_name = azurerm_storage_container.containerbronze.name
  type                   = "Block"
  source                 = "data/addresses.csv"
}