# Databricks notebook source
# MAGIC %run ../utils/dls_auth

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   dc.sexo,
# MAGIC   sum(fp.valor_total_compra) total
# MAGIC from
# MAGIC   db_gold.fact_pedido fp
# MAGIC   inner join db_gold.dim_produto dp on fp.cod_produto = dp.cod_produto
# MAGIC   inner join db_gold.dim_classe_produto dcp on dcp.cod_classe_produto = dp.cod_classe_produto
# MAGIC   inner join db_gold.dim_cliente dc on dc.cod_cliente = fp.cod_cliente
# MAGIC group by dc.sexo

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   dp.nome_produto,
# MAGIC   count(fp.cod_produto) qtd_vendida
# MAGIC from
# MAGIC   db_gold.fact_pedido fp
# MAGIC   inner join db_gold.dim_produto dp on fp.cod_produto = dp.cod_produto
# MAGIC   inner join db_gold.dim_classe_produto dcp on dcp.cod_classe_produto = dp.cod_classe_produto
# MAGIC   inner join db_gold.dim_cliente dc on dc.cod_cliente = fp.cod_cliente
# MAGIC group by dp.nome_produto
# MAGIC order by qtd_vendida desc
