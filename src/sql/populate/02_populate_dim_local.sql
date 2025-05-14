-- Popula DIM_LOCAL com UFs únicas da tabela de staging
INSERT INTO DIM_LOCAL (
    uf,
    latitude, 
    longitude 
)
SELECT
    DISTINCT s.purchase_location_code AS uf,
    MIN(s.latitude) AS latitude, 
    MIN(s.longitude) AS longitude 
FROM
    staging_produtos_ecommerce s
WHERE
    s.purchase_location_code IS NOT NULL
    AND s.purchase_location_code != 'n/a' -- evirtar inserir 'n/a' se for um default
    AND NOT EXISTS ( -- garantir que a UF ainda não exista na dimensão
        SELECT 1
        FROM DIM_LOCAL dl
        WHERE dl.uf = s.purchase_location_code
    )
GROUP BY
    s.purchase_location_code -- Necessário por causa das funções de agregação MIN()
ORDER BY
    s.purchase_location_code;