# Databricks notebook source
# MAGIC %run ../utils/deltalake_handler

# COMMAND ----------

from abc import ABC, abstractmethod

class Ingestion(ABC):

    @property
    @abstractmethod
    def database_name(self): pass


    @property
    @abstractmethod
    def table_name(self): pass


    @property
    @abstractmethod
    def source_connector(self): pass


    @abstractmethod
    def silver_transformer(self): pass


    @abstractmethod
    def silver_delta_writer(self, df): pass


    @property
    def bronze_delta_handler(self):
        return DeltalakeHandler("bronze", self.database_name, 
            self.table_name)
        

    @property
    def silver_delta_handler(self):
        return DeltalakeHandler("silver", self.database_name, 
            self.table_name)


    @property
    def read_source(self):
        return self.source_connector.load()
    
    
    @property
    def read_bronze(self):
        return self.bronze_delta_handler.read_table
    

    @property
    def read_silver(self):
        return self.silver_delta_handler.read_table


    def source_to_bronze(self):
        df = self.read_source
        self.bronze_delta_handler.overwrite(df)


    def bronze_to_silver(self):
        df = self.silver_transformer()
        self.silver_delta_writer(df)

    
    def run(self):
        self.source_to_bronze()
        self.bronze_to_silver()
