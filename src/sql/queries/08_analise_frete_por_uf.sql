/*
    -- Consulta 8: Análise de frete (frete médio, mínimo, máximo e total por UF)
    -- Objetivo: Entender os custos e a receita de frete por localização (UF).
*/
SELECT
    dl.uf,
    -- dl.regiao, -- Descomente se você adicionar e popular a coluna 'regiao' em DIM_LOCAL
    AVG(fv.shipping_cost_cents / 100.0) AS frete_medio,
    MIN(fv.shipping_cost_cents / 100.0) AS frete_minimo,
    MAX(fv.shipping_cost_cents / 100.0) AS frete_maximo,
    SUM(fv.shipping_cost_cents / 100.0) AS frete_total_uf
FROM
    FATO_VENDAS fv
JOIN
    DIM_LOCAL dl ON fv.sk_local = dl.sk_local
WHERE
    fv.shipping_cost_cents IS NOT NULL -- Considera apenas vendas com informação de frete
    AND dl.sk_local != -1              -- Exclui locais 'Desconhecidos'
GROUP BY
    dl.uf
    -- dl.regiao
ORDER BY
    frete_medio DESC;