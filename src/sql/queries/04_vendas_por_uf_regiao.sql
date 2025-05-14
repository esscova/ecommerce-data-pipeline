/*

    Consulta 4: Vendas por UF (e potencialmente Região, se populada) e avaliação média
    Objetivo: Entender a distribuição geográfica das vendas e a satisfação dos clientes por local.
    *Assumindo que DIM_LOCAL tem 'uf' e opcionalmente 'regiao'.

*/

SELECT
    dl.uf,
    COUNT(fv.sk_venda) AS total_transacoes_venda,
    SUM((fv.preco_cents + COALESCE(fv.shipping_cost_cents, 0)) / 100.0) AS valor_total_vendas,
    AVG(fv.purchase_rating) AS avaliacao_media_compra
FROM
    FATO_VENDAS fv
JOIN
    DIM_LOCAL dl ON fv.sk_local = dl.sk_local
WHERE dl.sk_local != -1 -- excluir vendas com local 'Desconhecido', se houver
GROUP BY
    dl.uf
ORDER BY
    valor_total_vendas DESC;