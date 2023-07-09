# Databricks notebook source
# MAGIC %run ./connector

# COMMAND ----------

class MongodbConnector(Connector):
    """"This class makes the connection with the 
    mongodb collections"""

    URI = dbutils.secrets.get("mongodb", "uri")

    def set_source(self):
        self.df = spark.read.format("mongo") \
            .option("uri", self.URI) \
            .option("database", database) \
            .option("collection", entity)
