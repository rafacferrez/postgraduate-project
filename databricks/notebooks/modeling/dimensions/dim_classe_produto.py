# Databricks notebook source
# MAGIC %run ../model_builder

# COMMAND ----------

class DimClasseProduto(ModelBuilder):

    @property
    def database_name(self): return "analytics"
    

    @property
    def table_name(self): return "dim_classe_produto"


    def gold_transformer(self):
        return self.read_silver("produto") \
            .select("cod_classe_produto", 
                "classe_produto") \
            .distinct()


    @property
    def gold_delta_handler(self):
        return DeltalakeHandler(self.layer, self.database_name,
            self.table_name)


    def gold_delta_writer(self, df):
        self.gold_delta_handler.overwrite(df)

# COMMAND ----------

DimClasseProduto().run()
