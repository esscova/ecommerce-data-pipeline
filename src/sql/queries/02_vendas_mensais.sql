/*
    Consulta 2: Vendas mensais ao longo do tempo
    Objetivo: Analisar tendências de vendas e receita mês a mês.

*/
SELECT
    dt.ano,
    dt.mes,
    dt.nome_mes,
    COUNT(fv.sk_venda) AS total_transacoes_venda,
    SUM((fv.preco_cents + COALESCE(fv.shipping_cost_cents, 0)) / 100.0) AS valor_total_vendas
FROM
    FATO_VENDAS fv
JOIN
    DIM_TEMPO dt ON fv.sk_tempo = dt.sk_tempo
GROUP BY
    dt.ano,
    dt.mes,
    dt.nome_mes
ORDER BY
    dt.ano,
    dt.mes;