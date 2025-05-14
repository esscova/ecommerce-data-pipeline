-- Popula DIM_PRODUTO com produtos únicos da tabela de staging
INSERT INTO DIM_PRODUTO (
    nome_produto,
    categoria
)
SELECT
    DISTINCT s.product_name AS nome_produto,
    s.category_name AS categoria
FROM
    staging_produtos_ecommerce s
WHERE
    s.product_name IS NOT NULL AND s.product_name != 'nome indisponível'
    AND s.category_name IS NOT NULL AND s.category_name != 'outros'
    AND NOT EXISTS (
        SELECT 1
        FROM DIM_PRODUTO dp
        WHERE dp.nome_produto = s.product_name
          AND dp.categoria = s.category_name
    )
ORDER BY
    s.product_name, s.category_name;