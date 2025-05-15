/*
    Consulta 6: Análise sazonal (vendas por trimestre e ano)
    Objetivo: Identificar padrões de vendas trimestrais e anuais para planejamento e estratégia.
*/
SELECT
    dt.ano,
    dt.trimestre,
    COUNT(fv.sk_venda) AS total_transacoes_venda,
    SUM((fv.preco_cents + COALESCE(fv.shipping_cost_cents, 0)) / 100.0) AS valor_total_vendas
FROM
    FATO_VENDAS fv
JOIN
    DIM_TEMPO dt ON fv.sk_tempo = dt.sk_tempo
GROUP BY
    dt.ano,
    dt.trimestre
ORDER BY
    dt.ano,
    dt.trimestre;