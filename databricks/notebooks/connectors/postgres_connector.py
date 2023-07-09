# Databricks notebook source
# MAGIC %run ./connector

# COMMAND ----------

class PostgresConnector(Connector):
    """"This class makes the connection with the 
    postgres database"""

    options = {
        "host": dbutils.secrets.get("postgres", "url")
        ,"port": dbutils.secrets.get("postgres", "port")
        ,"user": dbutils.secrets.get("postgres", "user")
        ,"password": dbutils.secrets.get("postgres", "password")
    }

    def set_source(self):
        self.df = spark.read \
            .format("postgresql") \
            .options(self.options) \
            .option("database", self.db) \
            .option("dbtable", self.entity)
