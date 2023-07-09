# Databricks notebook source
# MAGIC %run ../utils/dls_auth

# COMMAND ----------

# MAGIC %run ./connector

# COMMAND ----------

# MAGIC %run ../utils/path_handler

# COMMAND ----------

from pyspark.sql.functions import col

class DatalakeConnector(Connector):
    """This class makes the connection with the 
    data lake layers"""

    def __init__(self, layer, db, entity, file_format, options={}):
        super().__init__(db, entity)
        self.layer = layer
        self.file_format = file_format
        self.options = options
        
    def set_source(self):
        entity_path = PathHandler(self.layer, self.db, self.entity).get_path()
        self.options["path"] = entity_path

        self.df = spark.read \
            .options(**self.options)\
            .format(self.file_format)
