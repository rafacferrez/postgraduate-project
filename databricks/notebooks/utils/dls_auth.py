# Databricks notebook source
storage_account = dbutils.secrets.get("adls", "storage-account")

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", 
    "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", 
    dbutils.secrets.get("adls", "app-id"))
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", 
    dbutils.secrets.get("adls", "client-secret"))
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", 
    dbutils.secrets.get("adls", "ad-endpoint"))
