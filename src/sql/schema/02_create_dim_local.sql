-- tabela de dimensão de local
CREATE TABLE IF NOT EXISTS DIM_LOCAL (
    sk_local SERIAL PRIMARY KEY,
    uf CHAR(2) NOT NULL UNIQUE, 
    regiao VARCHAR(50), 
    latitude NUMERIC(10, 7), 
    longitude NUMERIC(10, 7),
    data_inclusao TIMESTAMP WITHOUT TIME ZONE DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    data_atualizacao TIMESTAMP WITHOUT TIME ZONE DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')
);
COMMENT ON TABLE DIM_LOCAL IS 'Dimensão de Localização (UF) das compras.';