-- tabela de dimensão de produto
CREATE TABLE IF NOT EXISTS DIM_PRODUTO (
    sk_produto SERIAL PRIMARY KEY,
    nome_produto TEXT NOT NULL,
    categoria VARCHAR(255) NOT NULL,
    brand VARCHAR(255),
    data_inclusao TIMESTAMP WITHOUT TIME ZONE DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    data_atualizacao TIMESTAMP WITHOUT TIME ZONE DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    CONSTRAINT uq_produto_negocio UNIQUE (nome_produto, categoria, brand) 
);
COMMENT ON TABLE DIM_PRODUTO IS 'Dimensão de Produtos.';
/*
como não tem um ID da fonte, seguira a regra de negocio de omitir ou deixar nulo e confiar na combinação de atributos.
*/