# Databricks notebook source
# MAGIC %run ../ingestion

# COMMAND ----------

# MAGIC %run ../../connectors/postgres_connector

# COMMAND ----------

class EntityProduto(Ingestion):

    @property
    def database_name(self): return "flatfile" 


    @property
    def table_name(self): return "produto"


    @property
    def source_connector(self):
        return PostgresConnector("postgres", "produto")
        
    
    def __cast_columns(self, df):
        return df.selectExpr("cast(cod_produto as int) as cod_produto",
            "cast(cod_produto as int) as cod_classe_produto",
            "cast(translate(valor, ',', '.') as double) as valor",
            "nome_produto", "classe_produto")


    def silver_transformer(self): 
        return self.read_bronze \
            .transform(self.__cast_columns)


    def silver_delta_writer(self, df):
        return self.silver_delta_handler.overwrite(df)

# COMMAND ----------

EntityProduto().run()
