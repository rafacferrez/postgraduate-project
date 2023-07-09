# Databricks notebook source
# MAGIC %run ../model_builder

# COMMAND ----------

class DimCliente(ModelBuilder):

    @property
    def database_name(self): return "analytics"
    

    @property
    def table_name(self): return "dim_cliente"


    def gold_transformer(self):
        return self.read_silver("cliente") \
            .selectExpr("cod_cliente", 
                "case sexo when 1 then 'M' else 'F' end as sexo", 
                "idade", "diabetes", "hipertenso", "qtd_filhos",
                """case estado_civil when 1 then 'solteiro'
                 when 2 then 'uniao_estavel'
                 when  3 then 'casado'
                 else 'viúvo' end as estado_civil""") \
            .distinct()


    @property
    def gold_delta_handler(self):
        return DeltalakeHandler(self.layer, self.database_name,
            self.table_name)


    def gold_delta_writer(self, df):
        self.gold_delta_handler.overwrite(df)

# COMMAND ----------

DimCliente().run()
