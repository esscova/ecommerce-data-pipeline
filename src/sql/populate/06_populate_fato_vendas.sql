-- Popula FATO_VENDAS a partir da tabela de staging e das dimensões
-- assumindo que a tabela fato é truncada antes ou que você tem uma lógica para evitar duplicatas.


INSERT INTO FATO_VENDAS (
    sk_produto,
    sk_vendedor,
    sk_local,
    sk_tempo,
    sk_pagamento,
    preco_cents,
    shipping_cost_cents,
    purchase_rating,
    etl_load_timestamp 
)
SELECT
    COALESCE(dp.sk_produto, -1) AS sk_produto,         -- Usar -1 ou um SK para 'Desconhecido' se o produto não for encontrado
    COALESCE(dv.sk_vendedor, -1) AS sk_vendedor,       -- Usar -1 ou um SK para 'Desconhecido'
    COALESCE(dl.sk_local, -1) AS sk_local,             -- Usar -1 ou um SK para 'Desconhecido'
    COALESCE(dt.sk_tempo, -1) AS sk_tempo,             -- Usar -1 ou um SK para 'Desconhecido'
    COALESCE(dpm.sk_pagamento, -1) AS sk_pagamento,    -- Usar -1 ou um SK para 'Desconhecido'
    s.price_cents,
    s.shipping_cost_cents,
    s.purchase_rating,
    s.etl_load_timestamp -- timestamp da carga na staging
FROM
    staging_produtos_ecommerce s
LEFT JOIN
    DIM_PRODUTO dp ON s.product_name = dp.nome_produto
                    AND s.category_name = dp.categoria
                    AND (s.brand = dp.brand OR (s.brand IS NULL AND dp.brand IS NULL)) -- ajuda se brand não for usado
LEFT JOIN
    DIM_VENDEDOR dv ON s.seller_name = dv.nome_vendedor
LEFT JOIN
    DIM_LOCAL dl ON s.purchase_location_code = dl.uf
LEFT JOIN
    DIM_TEMPO dt ON s.purchase_date = dt.data_completa
LEFT JOIN
    DIM_PAGAMENTO dpm ON s.payment_type = dpm.tipo_pagamento
                       AND (s.installments_quantity = dpm.qtd_parcelas OR (s.installments_quantity IS NULL AND dpm.qtd_parcelas IS NULL))
WHERE
    s.purchase_date IS NOT NULL; -- Um fato de venda geralmente requer uma data