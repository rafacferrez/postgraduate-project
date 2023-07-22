# Databricks notebook source
# MAGIC %run ./connector

# COMMAND ----------

class PostgresConnector(Connector):
    """"This class makes the connection with the 
    postgres database"""

    host = dbutils.secrets.get("psql", "host")

    def set_source(self):
        options = {
            "driver": "org.postgresql.Driver"
            ,"url": f"jdbc:postgresql://{self.host}:5432/{self.db}"
            ,"user": dbutils.secrets.get("psql", "user")
            ,"password": dbutils.secrets.get("psql", "passwd")
            ,"dbtable" : self.entity
        }

        self.df = spark.read \
            .format("jdbc") \
            .options(**options)
