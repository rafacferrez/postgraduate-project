# Databricks notebook source
# MAGIC %run ./dls_auth

# COMMAND ----------

# MAGIC %run ./path_handler

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import lit
import os

class DeltalakeHandler:
    
    def __init__(self,
                 layer,
                 database,
                 table,
                 merge_key=None,
                 *partition_params):
        self.layer = layer.lower()
        self.table = table.lower()
        self.database = database.lower()
        self.hive_database = f"db_{layer.lower()}"
        self.partition_params = partition_params


    @property
    def table_path(self):
      return PathHandler(self.layer, self.database,
          self.table).get_path()


    @property
    def read_table(self):
        return spark.read.load(self.table_path)
    

    def __set_cols_expr(self, cols):
        if isinstance(cols, (list, tuple)):
            for c in cols:
                cols = dict(zip(cols, ["{}{}".format("updates.", c) for c in cols]))
        return cols
    
    
    def __check_merge_key(self):
        if self.merge_key is None:
            raise Exception("Merge key not found.")
            
            
    def save(self, dataframe):
        dataframe \
            .write \
            .format("delta") \
            .partitionBy(self.partition_params) \
            .mode("ErrorIfExists") \
            .save(self.table_path)
        self.__check_hive_metastore()


    def overwrite(self, dataframe):
        if DeltaTable.isDeltaTable(spark, self.table_path):
            dataframe \
                .write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .partitionBy(self.partition_params) \
                .save(self.table_path)
        else:
            self.save(dataframe)
        self.__check_hive_metastore()


    def append(self, dataframe):
        if DeltaTable.isDeltaTable(spark, self.table_path):
            dataframe \
                .write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .partitionBy(self.partition_params) \
                .save(self.table_path)
        else:
            self.save(dataframe)
        self.__check_hive_metastore()


    def replace_partition(self, dataframe, partition_name, partition_value):
        if DeltaTable.isDeltaTable(spark, self.table_path):
            dataframe \
                .write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .option("replaceWhere", f"{partition_name} = '{partition_value}'") \
                .save(self.table_path)
        else:
            self.save(dataframe)
        self.__check_hive_metastore()


    def merge(self, dataframe, cols_to_update=None, ignore_target_columns=False):
        self.__check_merge_key()
            
        if DeltaTable.isDeltaTable(spark, self.table_path):
            
            if (cols_to_update is not None) & (ignore_target_columns):
                raise Exception("Merge not accepted with two parameters filled.")

            if ignore_target_columns == False:
                dataframe = self.__set_source_df(dataframe)

            if cols_to_update:
                spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", False)
                DeltaTable \
                    .forPath(spark, self.table_path) \
                    .alias("target") \
                    .merge(dataframe.alias("updates"), self.merge_key) \
                    .whenMatchedUpdate(set = self.__set_cols_expr(cols_to_update)) \
                    .whenNotMatchedInsert(values = self.__set_cols_expr(cols_to_update)) \
                    .execute()
            else:
                spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)
                DeltaTable \
                    .forPath(spark, self.table_path) \
                    .alias("target") \
                    .merge(dataframe.alias("updates"), self.merge_key) \
                    .whenMatchedUpdateAll() \
                    .whenNotMatchedInsertAll() \
                    .execute()
        else:
            self.save(dataframe)
        self.__check_hive_metastore()


    def insert(self, dataframe):
        """
        insert if not exists
        """
        self.__check_merge_key()
            
        if DeltaTable.isDeltaTable(spark, self.table_path):
            spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)
            DeltaTable \
                .forPath(spark, self.table_path) \
                .alias("target") \
                .merge(dataframe.alias("updates"), self.merge_key) \
                .whenNotMatchedInsertAll() \
                .execute()
        else:
            self.save(dataframe)

        self.__check_hive_metastore()


    def update(self, dataframe, cols_to_update):
        self.__check_merge_key()
        
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", False)
        if DeltaTable.isDeltaTable(spark, self.table_path):
            DeltaTable \
                .forPath(spark, self.table_path) \
                .alias("target") \
                .merge(dataframe.alias("updates"), self.merge_key) \
                .whenMatchedUpdate(set = self.__set_cols_expr(cols_to_update)) \
                .execute()
        else:
            raise Exception("Target not found.")
    
    
    def __check_hive_metastore(self):
        if self.layer != "bronze":
          self.__check_database()
          self.__check_external_table()
    
      
    def __check_database(self):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.hive_database}")
        

    def __check_external_table(self):
        spark.sql(f"CREATE TABLE IF NOT EXISTS {self.hive_database}.{self.table} USING DELTA LOCATION '{self.table_path}'")
