-- Databricks notebook source
WITH tb_join as (
  SELECT  DISTINCT
          t2.idVendedor,
          t3.*

  FROM silver.olist.pedido t1

  LEFT JOIN silver.olist.item_pedido t2
  ON t1.idPedido = t2.idPedido

  LEFT JOIN silver.olist.produto t3
  ON t2.idProduto = t3.idProduto

  WHERE t1.dtPedido < '2018-01-01'
  AND t1.dtPedido >= add_months('2018-01-01', -6)
  AND t2.idVendedor IS NOT NULL),
  

tb_summary as (

  SELECT  
      idVendedor,
      
      AVG(coalesce(nrFotos,0)) as avgFotos,
      AVG(VlComprimentoCm * vlAlturaCm * vlLarguraCm) as avgVolumeProduto,
      PERCENTILE(VlComprimentoCm * vlAlturaCm * vlLarguraCm, 0.5) as medianVolumeProduto,
      MAX(VlComprimentoCm * vlAlturaCm * vlLarguraCm) as maxVolumeProduto,
      MIN(VlComprimentoCm * vlAlturaCm * vlLarguraCm) as minVolumeProduto,
      
      
      count(distinct case when descCategoria = 'cama_mesa_banho' then idProduto end) / count(distinct idProduto) as pctCategoriaCamaMesaBanho,
      count(distinct case when descCategoria = 'beleza_saude' then idProduto end) / count(distinct idProduto) as pctCategoriaBelezaSaude,
      count(distinct case when descCategoria = 'esporte_lazer' then idProduto end) / count(distinct idProduto) as pctCategoriaEsporteLazer,
      count(distinct case when descCategoria = 'informatica_acessorios' then idProduto end) / count(distinct idProduto) as pctCategoriaInformaticaAcessorios,
      count(distinct case when descCategoria = 'moveis_decoracao' then idProduto end) / count(distinct idProduto) as pctCategoriaMoveisDecoracao,
      count(distinct case when descCategoria = 'utilidades_domesticas' then idProduto end) / count(distinct idProduto) as pctCategoriaUtilidadesDomestica,
      count(distinct case when descCategoria = 'relogios_presentes' then idProduto end) / count(distinct idProduto) as pctCategoriaRelogioPresentes,
      count(distinct case when descCategoria = 'telefonia' then idProduto end) / count(distinct idProduto) as pctCategoriaTelefonia,
      count(distinct case when descCategoria = 'automotivo' then idProduto end) / count(distinct idProduto) as pctCategoriaAutomotivo,
      count(distinct case when descCategoria = 'brinquedos' then idProduto end) / count(distinct idProduto) as pctCategoriaBrinquedos,
      count(distinct case when descCategoria = 'cool_stuff' then idProduto end) / count(distinct idProduto) as pctCategoriaCoolStuff,
      count(distinct case when descCategoria = 'ferramentas_jardim' then idProduto end) / count(distinct idProduto) as pctCategoriaFerramentasJardim,
      count(distinct case when descCategoria = 'perfumaria' then idProduto end) / count(distinct idProduto) as pctCategoriaPerfumaria,
      count(distinct case when descCategoria = 'bebes' then idProduto end) / count(distinct idProduto) as pctCategoriaBebes,
      count(distinct case when descCategoria = 'eletronicos' then idProduto end) / count(distinct idProduto) as pctCategoriaEletronicos

  FROM tb_join
  GROUP BY 1)
  
  SELECT 
        '2018-01-01' as dtReference,
        *
  FROM tb_summary

-- COMMAND ----------

  SELECT  descCategoria
        
  FROM silver.olist.item_pedido t2

  LEFT JOIN silver.olist.produto t3
  ON t2.idProduto = t3.idProduto

  WHERE t2.idVendedor IS NOT NULL
  GROUP BY 1
  ORDER BY count(distinct idPedido)  desc
  limit 15
