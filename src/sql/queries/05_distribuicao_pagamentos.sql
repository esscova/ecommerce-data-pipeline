/*
    Consulta 5: Distribuição de vendas por tipo de pagamento e quantidade de parcelas
    Objetivo: Analisar as formas de pagamento mais utilizadas pelos clientes e a popularidade das opções de parcelamento.
*/
SELECT
    dp.tipo_pagamento,
    COALESCE(dp.qtd_parcelas, 0) AS quantidade_parcelas, -- Trata NULL em parcelas como 0 para agrupamento
    COUNT(fv.sk_venda) AS total_transacoes_venda,
    SUM((fv.preco_cents + COALESCE(fv.shipping_cost_cents, 0)) / 100.0) AS valor_total_vendas,
    AVG((fv.preco_cents + COALESCE(fv.shipping_cost_cents, 0)) / 100.0) AS valor_medio_por_transacao
FROM
    FATO_VENDAS fv
JOIN
    DIM_PAGAMENTO dp ON fv.sk_pagamento = dp.sk_pagamento
WHERE
    dp.sk_pagamento != -1 -- Exclui pagamentos 'Desconhecido', se houver
GROUP BY
    dp.tipo_pagamento,
    COALESCE(dp.qtd_parcelas, 0)
ORDER BY
    dp.tipo_pagamento,
    quantidade_parcelas;