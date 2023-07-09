# Databricks notebook source
# MAGIC %run ../ingestion

# COMMAND ----------

# MAGIC %run ../../connectors/datalake_connector

# COMMAND ----------

from pyspark.sql.functions import col

class EntityCliente(Ingestion):

    @property
    def database_name(self): return "flatfile" 


    @property
    def table_name(self): return "cliente"


    @property
    def source_connector(self):
        options = {"multiline": True}
        return DatalakeConnector("bronze", self.database_name,
            "example_cliente", "json", options)
        
    def __explode_cols(self, df):
        return df \
            .selectExpr("explode(cliente) as cliente") \
        
    
    def __extract_cols(self, df):
        cols = ["cod_cliente", "cod_estado", "diabetes",
        "estado_civil", "hipertenso", "idade",
        "qtd_filhos", "sexo"]
        for col_name in cols:
            df = df \
                .withColumn(col_name, col(f"cliente.{col_name}"))
        return df.drop("cliente")


    def silver_transformer(self): 
        return self.read_bronze \
            .transform(self.__explode_cols) \
            .transform(self.__extract_cols)


    def silver_delta_writer(self, df):
        return self.silver_delta_handler.overwrite(df)

# COMMAND ----------

EntityCliente().run()
