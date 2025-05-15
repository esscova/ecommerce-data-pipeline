/*
    Consulta 11: Desempenho de vendas mensais por vendedor
    Objetivo: Acompanhar a evolução das vendas de cada vendedor ao longo do tempo para identificar tendências e destaques.
*/

SELECT
    dt.ano,
    dt.mes,
    dt.nome_mes,
    dv.nome_vendedor,
    COUNT(fv.sk_venda) AS total_transacoes_venda,
    SUM((fv.preco_cents + COALESCE(fv.shipping_cost_cents, 0)) / 100.0) AS valor_total_vendas
FROM
    FATO_VENDAS fv
JOIN
    DIM_TEMPO dt ON fv.sk_tempo = dt.sk_tempo
JOIN
    DIM_VENDEDOR dv ON fv.sk_vendedor = dv.sk_vendedor
WHERE
    dv.sk_vendedor != -1 -- Exclui vendedores 'Desconhecidos'
GROUP BY
    dt.ano,
    dt.mes,
    dt.nome_mes,
    dv.nome_vendedor
ORDER BY
    dt.ano,
    dt.mes,
    dv.nome_vendedor, -- Adicionado para melhor ordenação dentro do mês
    valor_total_vendas DESC;