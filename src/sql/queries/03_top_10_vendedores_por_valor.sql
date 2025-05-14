/*

    Consulta 3: Top 10 vendedores por valor total de vendas
    Objetivo: Identificar os vendedores com melhor desempenho em termos de receita gerada.

*/
SELECT
    dv.nome_vendedor,
    COUNT(fv.sk_venda) AS total_transacoes_venda,
    SUM((fv.preco_cents + COALESCE(fv.shipping_cost_cents, 0)) / 100.0) AS valor_total_vendas
FROM
    FATO_VENDAS fv
JOIN
    DIM_VENDEDOR dv ON fv.sk_vendedor = dv.sk_vendedor
WHERE dv.sk_vendedor != -1 -- Exclui vendas com vendedor 'Desconhecido', se houver
GROUP BY
    dv.nome_vendedor
ORDER BY
    valor_total_vendas DESC
LIMIT 10;