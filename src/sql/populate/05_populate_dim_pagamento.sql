-- Popula DIM_PAGAMENTO com combinações únicas de tipo_pagamento e qtd_parcelas
INSERT INTO DIM_PAGAMENTO (
    tipo_pagamento,
    qtd_parcelas
)
SELECT
    DISTINCT s.payment_type AS tipo_pagamento,
    s.installments_quantity AS qtd_parcelas
FROM
    staging_produtos_ecommerce s
WHERE
    s.payment_type IS NOT NULL AND s.payment_type != 'não especificado'
    AND NOT EXISTS (
        SELECT 1
        FROM DIM_PAGAMENTO dp
        WHERE dp.tipo_pagamento = s.payment_type
          AND (dp.qtd_parcelas = s.installments_quantity OR (dp.qtd_parcelas IS NULL AND s.installments_quantity IS NULL))
    )
ORDER BY
    s.payment_type, s.installments_quantity;