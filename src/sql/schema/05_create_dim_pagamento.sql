-- tabela de dimensão de pagamento
CREATE TABLE IF NOT EXISTS DIM_PAGAMENTO (
    sk_pagamento SERIAL PRIMARY KEY,
    tipo_pagamento VARCHAR(50) NOT NULL,
    qtd_parcelas INTEGER, -- pode ser nulo .. ex.: boleto
    data_inclusao TIMESTAMP WITHOUT TIME ZONE DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    data_atualizacao TIMESTAMP WITHOUT TIME ZONE DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    CONSTRAINT uq_pagamento_negocio UNIQUE (tipo_pagamento, qtd_parcelas)
);
COMMENT ON TABLE DIM_PAGAMENTO IS 'Dimensão de Tipos e Condições de Pagamento.';