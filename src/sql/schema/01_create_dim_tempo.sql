-- tabela de dimensão de tempo
CREATE TABLE IF NOT EXISTS DIM_TEMPO (
    sk_tempo SERIAL PRIMARY KEY,
    data_completa DATE NOT NULL UNIQUE,
    ano INT NOT NULL,
    mes INT NOT NULL,
    dia INT NOT NULL,
    nome_dia_semana VARCHAR(20),
    nome_mes VARCHAR(20),
    trimestre INT,
    semestre INT,
    data_inclusao TIMESTAMP WITHOUT TIME ZONE DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')
);
CREATE INDEX IF NOT EXISTS idx_dim_tempo_data_completa ON DIM_TEMPO(data_completa);
COMMENT ON TABLE DIM_TEMPO IS 'Dimensão de Tempo para análise de vendas.';