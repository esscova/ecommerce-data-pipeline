-- Popula DIM_TEMPO com datas únicas da tabela de staging .. abordagem simples

INSERT INTO DIM_TEMPO (
    data_completa,
    ano,
    mes,
    dia,
    nome_dia_semana,
    nome_mes,
    trimestre,
    semestre
)
SELECT
    DISTINCT s.purchase_date AS data_completa,
    EXTRACT(YEAR FROM s.purchase_date) AS ano,
    EXTRACT(MONTH FROM s.purchase_date) AS mes,
    EXTRACT(DAY FROM s.purchase_date) AS dia,
    TO_CHAR(s.purchase_date, 'Day') AS nome_dia_semana, 
    TO_CHAR(s.purchase_date, 'Month') AS nome_mes,     
    EXTRACT(QUARTER FROM s.purchase_date) AS trimestre,
    CASE
        WHEN EXTRACT(MONTH FROM s.purchase_date) <= 6 THEN 1
        ELSE 2
    END AS semestre
FROM
    staging_produtos_ecommerce s
WHERE
    s.purchase_date IS NOT NULL
    AND NOT EXISTS ( -- garantir que a data ainda não exista na dimensão
        SELECT 1
        FROM DIM_TEMPO dt
        WHERE dt.data_completa = s.purchase_date
    )
ORDER BY data_completa;
