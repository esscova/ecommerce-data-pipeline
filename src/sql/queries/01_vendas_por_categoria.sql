/* 
    
    Consulta 1: Total de vendas e valor por categoria de produto
    Objetivo: Entender quais categorias de produtos são mais populares e geram mais receita.

*/
SELECT
    dp.categoria,
    COUNT(fv.sk_venda) AS total_transacoes_venda, -- Contando transações de venda
    SUM(fv.preco_cents / 100.0) AS valor_produtos_vendidos, -- Soma dos preços dos produtos
    SUM((fv.preco_cents + COALESCE(fv.shipping_cost_cents, 0)) / 100.0) AS valor_total_com_frete,
    AVG((fv.preco_cents + COALESCE(fv.shipping_cost_cents, 0)) / 100.0) AS ticket_medio_por_transacao
FROM
    FATO_VENDAS fv
JOIN
    DIM_PRODUTO dp ON fv.sk_produto = dp.sk_produto
GROUP BY
    dp.categoria
ORDER BY
    valor_total_com_frete DESC;

COMMENT ON TABLE FATO_VENDAS IS 'Atualizando comentário para incluir a ideia de que preco_cents é por item/transação.';