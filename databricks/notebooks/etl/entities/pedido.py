# Databricks notebook source
# MAGIC %run ../ingestion

# COMMAND ----------

# MAGIC %run ../../connectors/postgres_connector

# COMMAND ----------

from pyspark.sql.functions import col, when, translate

class EntityPedido(Ingestion):

    @property
    def database_name(self): return "flatfile" 


    @property
    def table_name(self): return "pedido"


    @property
    def source_connector(self):
        return PostgresConnector("postgres", "pedido")
        
    
    def __cast_to_int(self, df):
        cols = ["cod_cliente", "cod_produto", "qtd_produto"]
        for col_name in cols:
            df = df.withColumn(col_name, col(col_name).cast("int"))
        return df
    
    
    def __cast_to_double(self, df):
        cols = ["valor_unitario", "valor_total_compra"]
        for col_name in cols:
            df = df.withColumn(col_name, 
                translate(col(col_name), ",", ".").cast("double"))
        return df
    

    def __missing_values(self, df):
        return df \
            .withColumn("valor_unitario", 
                when(col("valor_unitario").isNull(),
                    col("valor_total_compra") / col("qtd_produto"))
                .otherwise(col("valor_unitario"))) \
            .withColumn("valor_total_compra",
                when(col("valor_total_compra").isNull(),
                    col("valor_unitario") * col("qtd_produto"))
                .otherwise(col("valor_total_compra")))
    

    def silver_transformer(self): 
        return self.read_bronze \
            .transform(self.__cast_to_int) \
            .transform(self.__cast_to_double) \
            .transform(self.__missing_values)


    def silver_delta_writer(self, df):
        return self.silver_delta_handler.overwrite(df)

# COMMAND ----------

EntityPedido().run()
