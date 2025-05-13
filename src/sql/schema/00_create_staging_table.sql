-- tabela de staging para dados transformados de produtos
CREATE TABLE IF NOT EXISTS staging_produtos_ecommerce (
    staging_id SERIAL PRIMARY KEY,
    product_id TEXT, -- pode ser nulo, pois a API não fornece
    product_name TEXT,
    category_name VARCHAR(255),
    price_cents INTEGER,
    shipping_cost_cents INTEGER,
    purchase_date DATE,
    seller_name TEXT,
    purchase_location_code VARCHAR(10),
    purchase_rating INTEGER,
    payment_type VARCHAR(50),
    installments_quantity INTEGER,
    latitude NUMERIC(10, 7),
    longitude NUMERIC(10, 7),
    etl_load_timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL
);

COMMENT ON TABLE staging_produtos_ecommerce IS 'Tabela de staging para dados de produtos transformados antes da carga no Data Warehouse.';
COMMENT ON COLUMN staging_produtos_ecommerce.staging_id IS 'Chave primária auto-incrementada para a tabela de staging.';
COMMENT ON COLUMN staging_produtos_ecommerce.product_id IS 'Identificador do produto (pode ser nulo se não fornecido pela API).';
COMMENT ON COLUMN staging_produtos_ecommerce.etl_load_timestamp IS 'Timestamp de quando o registro foi carregado pelo ETL na staging.';