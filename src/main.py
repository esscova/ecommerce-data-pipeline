"""
Orquestrador do pipeline.

Fases:
1. Coleta de dados da API.
2. Armazenamento de dados brutos no MongoDB.
3. Extração dos dados brutos do MongoDB.
4. Transformação dos dados extraídos (product_id será None).
5. Carga dos dados transformados na tabela de staging do PostgreSQL (com staging_id SERIAL PK).
"""

# --- bibliotecas
import logging
import os
from dotenv import load_dotenv
from datetime import datetime # Para STAGING_COLUMN_ORDER se você tiver objetos datetime

# Ajuste os caminhos de importação conforme sua estrutura
from pipeline.api_data_extractor import APIExtractor
from pipeline.transform_data import Transform
from db.mongo_manager import MongoManager
from db.postgres_manager import PostgresManager

# --- configuração de logging
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO').upper(),
    format='%(asctime)s - %(levelname)s - [%(name)s:%(funcName)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger(__name__)

# --- carregar variáveis de ambiente
load_dotenv()
logger.info('Variáveis de ambiente carregadas.')

# --- variáveis padrão e de configuração do pipeline
API_BASE_URL = os.getenv('API_BASE_URL')

MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB_NAME = os.getenv('MONGO_DB')
MONGO_RAW_COLLECTION_NAME = os.getenv('MONGO_RAW_COLLECTION')

POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_DB_NAME = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_STAGING_TABLE_NAME = os.getenv('POSTGRES_STAGING_TABLE', 'staging_produtos_ecommerce')

# Definição da estrutura da tabela de staging com uma chave primária SERIAL
STAGING_TABLE_COLUMNS_SQL = """
    staging_id SERIAL PRIMARY KEY,  -- Chave primária auto-incrementada pelo PostgreSQL
    product_id TEXT,                -- Virá como None da transformação, pois a API não fornece
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
    latitude NUMERIC(10, 7),        -- 10 dígitos no total, 7 após o ponto decimal
    longitude NUMERIC(10, 7),
    etl_load_timestamp TIMESTAMP WITHOUT TIME ZONE
"""

# A ordem das colunas para inserção. NÃO inclua 'staging_id'.
# Deve corresponder às chaves dos dicionários em 'transformed_data'.
STAGING_COLUMN_ORDER = [
    "product_id", "product_name", "category_name", "price_cents", "shipping_cost_cents",
    "purchase_date", "seller_name", "purchase_location_code", "purchase_rating",
    "payment_type", "installments_quantity", "latitude", "longitude", "etl_load_timestamp"
]

# --- execução do pipeline
def main():
    logger.info("==================================================")
    logger.info(" Iniciando Pipeline - Todas as Fases")
    logger.info("==================================================")

    # Validações de variáveis de ambiente (simplificado para brevidade, mantenha suas validações)
    required_env_vars = {
        "API_BASE_URL": API_BASE_URL, "MONGO_URI": MONGO_URI, "MONGO_DB_NAME": MONGO_DB_NAME,
        "MONGO_RAW_COLLECTION_NAME": MONGO_RAW_COLLECTION_NAME, "POSTGRES_HOST": POSTGRES_HOST,
        "POSTGRES_DB_NAME": POSTGRES_DB_NAME, "POSTGRES_USER": POSTGRES_USER, "POSTGRES_PASSWORD": POSTGRES_PASSWORD
    }
    for var_name, var_value in required_env_vars.items():
        if not var_value:
            logger.error(f"Variável de ambiente '{var_name}' não definida. Encerrando.")
            return

    api_extractor = APIExtractor(url=API_BASE_URL)
    data_transformer = Transform()
    api_data = []
    raw_data_from_mongo = []
    transformed_data = []

    # Etapa 1: Extraindo dados da API
    try:
        logger.info("--- Etapa 1: Extraindo dados da API ---")
        api_data = api_extractor.get_data()
        if not api_data: logger.warning("Nenhum dado extraído da API.")
        else: logger.info(f"Quantidade de dados extraídos da API: {len(api_data)}")
    except Exception as e:
        logger.error(f"Erro crítico ao extrair dados da API: {e}", exc_info=True)
        return

    # Etapa 2: Carregando dados brutos no MongoDB
    if api_data:
        try:
            logger.info(f"--- Etapa 2: Carregando dados brutos no MongoDB (DB: {MONGO_DB_NAME}, Coleção: {MONGO_RAW_COLLECTION_NAME}) ---")
            with MongoManager(mongo_uri=MONGO_URI, db_name=MONGO_DB_NAME) as mongo_manager_load:
                if mongo_manager_load.set_active_collection(MONGO_RAW_COLLECTION_NAME):
                    logger.info(f"Coleção ativa para carga/limpeza: {mongo_manager_load.collection_name}")
                    if not mongo_manager_load.delete_all_documents():
                        logger.error(f"Falha ao limpar a coleção '{MONGO_RAW_COLLECTION_NAME}'. Encerrando.")
                        return
                    if mongo_manager_load.add_data(api_data):
                        logger.info(f"Dados brutos carregados com sucesso na coleção '{MONGO_RAW_COLLECTION_NAME}'.")
                    else:
                        logger.error(f"Falha ao adicionar dados brutos à '{MONGO_RAW_COLLECTION_NAME}'. Encerrando.")
                        return
                else:
                    logger.error(f"Falha ao definir coleção ativa '{MONGO_RAW_COLLECTION_NAME}'. Encerrando.")
                    return
        except Exception as e:
            logger.error(f"Erro na Etapa 2 (Carga MongoDB): {e}", exc_info=True)
            return
    else:
        logger.info("Nenhum dado da API para carregar no MongoDB. Encerrando pipeline.")
        return

    # Etapa 3: Extraindo dados brutos do MongoDB para transformação
    try:
        logger.info(f"--- Etapa 3: Extraindo dados brutos do MongoDB (DB: {MONGO_DB_NAME}, Coleção: {MONGO_RAW_COLLECTION_NAME}) ---")
        with MongoManager(mongo_uri=MONGO_URI, db_name=MONGO_DB_NAME) as mongo_manager_extract:
            raw_data_from_mongo = mongo_manager_extract.extract_data(source_collection_name=MONGO_RAW_COLLECTION_NAME)
            if not raw_data_from_mongo: logger.warning(f"Nenhum dado bruto extraído da '{MONGO_RAW_COLLECTION_NAME}'.")
            else: logger.info(f"{len(raw_data_from_mongo)} registros brutos extraídos do MongoDB.")
    except Exception as e:
        logger.error(f"Erro na Etapa 3 (Extração MongoDB): {e}", exc_info=True)
        return

    # Etapa 4: Transformando os Dados
    if raw_data_from_mongo:
        try:
            logger.info("--- Etapa 4: Transformando os dados ---")
            transformed_data = data_transformer.transform_data(raw_data_from_mongo)
            if not transformed_data: logger.warning("Nenhum dado retornado após a transformação.")
            else: logger.info(f"{len(transformed_data)} registros transformados com sucesso.")
        except Exception as e:
            logger.error(f"Erro crítico durante a transformação dos dados: {e}", exc_info=True)
            return
    else:
        logger.info("Nenhum dado extraído do MongoDB para transformação. Encerrando pipeline.")
        return

    # Etapa 5: Carregar Dados Transformados na Tabela de Staging do PostgreSQL
    if transformed_data:
        logger.info(f"--- Etapa 5: Carregando dados transformados na tabela de staging PostgreSQL ('{POSTGRES_STAGING_TABLE_NAME}') ---")
        try:
            with PostgresManager(db_host=POSTGRES_HOST,
                                 db_port=POSTGRES_PORT, 
                                 db_name=POSTGRES_DB_NAME,
                                 db_user=POSTGRES_USER,
                                 db_password=POSTGRES_PASSWORD) as pg_manager:

                pg_manager.create_table_if_not_exists(POSTGRES_STAGING_TABLE_NAME, STAGING_TABLE_COLUMNS_SQL)
                pg_manager.truncate_table(POSTGRES_STAGING_TABLE_NAME)

                if pg_manager.load_data_to_staging(POSTGRES_STAGING_TABLE_NAME, transformed_data, STAGING_COLUMN_ORDER):
                    logger.info(f"Dados transformados carregados com sucesso na tabela de staging '{POSTGRES_STAGING_TABLE_NAME}'.")
                else:
                    logger.error(f"Falha ao carregar dados transformados na tabela de staging '{POSTGRES_STAGING_TABLE_NAME}'. Encerrando.")
                    return
        except Exception as e: 
            logger.error(f"Erro na Etapa 5 (Carga PostgreSQL Staging): {e}", exc_info=True)
            return
    else:
        logger.info("Nenhum dado transformado disponível para carregar no PostgreSQL.")

    logger.info("==================================================")
    logger.info(" Pipeline Completo Concluído com Sucesso")
    logger.info("==================================================")

if __name__ == "__main__":
    main()