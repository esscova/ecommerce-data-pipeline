-- tabela fato
CREATE TABLE IF NOT EXISTS FATO_VENDAS (
    -- sk_venda SERIAL PRIMARY KEY, -- Comentado para mostrar a alternativa abaixo
    sk_produto INT NOT NULL REFERENCES DIM_PRODUTO(sk_produto),
    sk_vendedor INT NOT NULL REFERENCES DIM_VENDEDOR(sk_vendedor),
    sk_local INT NOT NULL REFERENCES DIM_LOCAL(sk_local),
    sk_tempo INT NOT NULL REFERENCES DIM_TEMPO(sk_tempo),
    sk_pagamento INT NOT NULL REFERENCES DIM_PAGAMENTO(sk_pagamento),
    preco_cents INTEGER,
    shipping_cost_cents INTEGER,
    purchase_rating INT,
    etl_load_timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL, -- Vírgula aqui se sk_venda for a próxima coluna
    sk_venda SERIAL PRIMARY KEY -- SEM vírgula aqui se for a última definição antes de fechar ')'
);

-- Índices e Comentários ...

-- Índices em chaves estrangeiras da tabela fato são cruciais para performance de JOIN
CREATE INDEX IF NOT EXISTS idx_fato_vendas_sk_produto ON FATO_VENDAS(sk_produto);
CREATE INDEX IF NOT EXISTS idx_fato_vendas_sk_vendedor ON FATO_VENDAS(sk_vendedor);
CREATE INDEX IF NOT EXISTS idx_fato_vendas_sk_local ON FATO_VENDAS(sk_local);
CREATE INDEX IF NOT EXISTS idx_fato_vendas_sk_tempo ON FATO_VENDAS(sk_tempo);
CREATE INDEX IF NOT EXISTS idx_fato_vendas_sk_pagamento ON FATO_VENDAS(sk_pagamento);

COMMENT ON TABLE FATO_VENDAS IS 'Tabela Fato centralizando as métricas de vendas e referenciando as dimensões.';