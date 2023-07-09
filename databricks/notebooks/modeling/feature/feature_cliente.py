# Databricks notebook source
# MAGIC %run ../model_builder

# COMMAND ----------

class FeatureCliente(ModelBuilder):

    @property
    def database_name(self): return "feature"


    @property
    def table_name(self): return "feature_cliente"


    @property
    def orders(self):
        return spark.sql(
        """
            select
                cod_cliente,
                sum(valor_total_compra) as vlr_total_gasto,
                max(valor_total_compra) as max_valor_compra,
                min(valor_total_compra) as min_valor_compra,
                count(cod_cliente) as qtd_pedidos,
                max(qtd_produto) as max_qtd_produto,
                min(qtd_produto) as min_qtd_produto,
                max(valor_unitario) as max_vlr_unitario,
                min(valor_unitario) as min_vlr_unitario
            from db_silver.pedido
            group by cod_cliente          
        """)


    def gold_transformer(self):
        return self.read_silver("cliente") \
            .join(self.orders, "cod_cliente") \
            .distinct()


    @property
    def gold_delta_handler(self):
        return DeltalakeHandler(self.layer, self.database_name,
            self.table_name)


    def gold_delta_writer(self, df):
        self.gold_delta_handler.overwrite(df)

# COMMAND ----------

FeatureCliente().run()
