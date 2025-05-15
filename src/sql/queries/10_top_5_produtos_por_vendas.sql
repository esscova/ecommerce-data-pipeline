/*
    Consulta 10: Top 5 produtos mais vendidos (por valor total de vendas)
    Objetivo: Identificar os produtos individuais que mais contribuem para a receita.
*/
SELECT
    dp.nome_produto,
    dp.categoria,
    -- Se cada linha em FATO_VENDAS representa uma unidade vendida ou uma transação única por produto:
    COUNT(fv.sk_venda) AS total_transacoes_ou_unidades_vendidas,
    SUM((fv.preco_cents + COALESCE(fv.shipping_cost_cents, 0)) / 100.0) AS valor_total_vendas
FROM
    FATO_VENDAS fv
JOIN
    DIM_PRODUTO dp ON fv.sk_produto = dp.sk_produto
WHERE
    dp.sk_produto != -1 -- Exclui produtos 'Desconhecidos'
GROUP BY
    dp.nome_produto,
    dp.categoria
ORDER BY
    valor_total_vendas DESC
LIMIT 5;