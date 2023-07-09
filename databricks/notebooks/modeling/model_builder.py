# Databricks notebook source
# MAGIC %run ../utils/deltalake_handler

# COMMAND ----------

from abc import ABC, abstractmethod

class ModelBuilder(ABC):

    @property
    def layer(self): return "gold"
    

    @property
    @abstractmethod
    def database_name(self): pass


    @property
    @abstractmethod
    def table_name(self): pass


    @abstractmethod
    def gold_transformer(self): pass


    @property
    @abstractmethod
    def gold_delta_handler(self): pass


    @property
    @abstractmethod
    def gold_delta_writer(self): pass


    def read_silver(self, table_name):
        return spark.table(f"db_silver.{table_name}")
    

    def read_gold(self, table_name):
        return spark.table(f"db_gold.{table_name}")
    

    def run(self):
        df = self.gold_transformer()
        self.gold_delta_writer(df)
