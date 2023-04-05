-- Databricks notebook source
select *
from silver.olist.pagamento_pedido

-- COMMAND ----------

WITH tb_join AS (
                  SELECT t2.*,
                         t3.idVendedor

                  FROM silver.olist.pedido t1

                  LEFT JOIN silver.olist.pagamento_pedido t2 ON t1.idPedido = t2.idPedido

                  LEFT JOIN silver.olist.item_pedido t3 ON t1.idPedido = t3.idPedido


                  WHERE t1.dtPedido < '2018-01-01'
                  AND t1.dtPedido >= add_months('2018-01-01', -6)
                  AND t3.idVendedor IS NOT NULL
),

tb_group AS (
              SELECT  idVendedor,
                      descTipoPagamento,
                      count(distinct idPedido) as qtdePedidoMeioPagamento,
                      sum(vlPagamento) as vlPedidoMeioPagamento
              FROM tb_join
              GROUP BY 1, 2
              ORDER BY 1, 2
)

SELECT  idVendedor,

        sum(case when descTipoPagamento = 'boleto' then qtdePedidoMeioPagamento else 0 end) as qtde_boleto,
        sum(case when descTipoPagamento = 'credit_card' then qtdePedidoMeioPagamento else 0 end) as qtde_credit_card,
        sum(case when descTipoPagamento = 'voucher' then qtdePedidoMeioPagamento else 0 end) as qtde_voucher,
        sum(case when descTipoPagamento = 'debit_card' then qtdePedidoMeioPagamento else 0 end) as qtde_debit_card,

        sum(case when descTipoPagamento = 'boleto' then VlPedidoMeioPagamento else 0 end) as vl_boleto,
        sum(case when descTipoPagamento = 'credit_card' then VlPedidoMeioPagamento else 0 end) as vl_credit_card,
        sum(case when descTipoPagamento = 'voucher' then VlPedidoMeioPagamento else 0 end) as vl_voucher,
        sum(case when descTipoPagamento = 'debit_card' then VlPedidoMeioPagamento else 0 end) as vl_debit_card,
        
        sum(case when descTipoPagamento = 'boleto' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtde_boleto,
        sum(case when descTipoPagamento = 'credit_card' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtde_credit_card,
        sum(case when descTipoPagamento = 'voucher' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtde_voucher,
        sum(case when descTipoPagamento = 'debit_card' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtde_debit_card,
        
        
        sum(case when descTipoPagamento = 'boleto' then VlPedidoMeioPagamento else 0 end) / sum(VlPedidoMeioPagamento) as pct_vl_boleto,
        sum(case when descTipoPagamento = 'credit_card' then VlPedidoMeioPagamento else 0 end) / sum(VlPedidoMeioPagamento) as pct_vl_credit_card,
        sum(case when descTipoPagamento = 'voucher' then VlPedidoMeioPagamento else 0 end) / sum(VlPedidoMeioPagamento) as pct_vl_voucher,
        sum(case when descTipoPagamento = 'debit_card' then VlPedidoMeioPagamento else 0 end) / sum(VlPedidoMeioPagamento) as pct_vl_debit_card
        

FROM tb_group
GROUP BY 1

-- COMMAND ----------


