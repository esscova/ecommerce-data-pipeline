-- Popula DIM_VENDEDOR com vendedores únicos da tabela de staging
INSERT INTO DIM_VENDEDOR (
    nome_vendedor
)
SELECT
    DISTINCT s.seller_name AS nome_vendedor
FROM
    staging_produtos_ecommerce s
WHERE
    s.seller_name IS NOT NULL
    AND s.seller_name != 'vendedor desconhecido' -- evitar inserir o default se não for um vendedor real
    AND NOT EXISTS ( -- garantir que o vendedor ainda não exista na dimensão
        SELECT 1
        FROM DIM_VENDEDOR dv
        WHERE dv.nome_vendedor = s.seller_name
    )
ORDER BY
    s.seller_name;