terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.10.0"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 1.6.0"
    }
    random  = "~> 2.3.1"
    databricks = {
      source = "databricks/databricks"
      version = "~> 1.4.0"
    }
  }
}

provider "azurerm" {
  features {
    resource_group {
        prevent_deletion_if_contains_resources = false
      }
  }
}

provider "databricks" {
    azure_workspace_resource_id = azurerm_databricks_workspace.databricks_workspace.id
    azure_use_msi = false
}
