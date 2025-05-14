"""
Orquestrador do pipeline ETL completo.

Fases:
0. Setup do Esquema do Banco de Dados PostgreSQL (Staging, Dimensões, Fato).
1. Coleta de dados da API.
2. Armazenamento de dados brutos no MongoDB.
3. Extração dos dados brutos do MongoDB.
4. Transformação dos dados extraídos.
5. Carga dos dados transformados na tabela de staging do PostgreSQL.
6. Populacão das tabelas de Dimensão e Fato a partir da Staging.
"""

# --- bibliotecas
import logging
import os
from dotenv import load_dotenv
import psycopg2 

from pipeline.api_data_extractor import APIExtractor
from pipeline.transform_data import Transform
from db.mongo_manager import MongoManager
from db.postgres_manager import PostgresManager

# --- configuração de logging ---
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(levelname)s - [%(name)s:%(module)s:%(funcName)s:%(lineno)d] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__) 

# --- carregar variáveis de ambiente ---
load_dotenv()
logger.info("Variáveis de ambiente carregadas.")

# --- Configurações Globais do Pipeline ---
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

# Caminhos para os diretórios com os scripts SQL
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) # diretório do main.py (src/)
SCHEMA_SCRIPTS_DIR = os.path.join(BASE_DIR, "sql", "schema")
POPULATE_DW_SCRIPTS_DIR = os.path.join(BASE_DIR, "sql", "populate")

# Ordem das colunas para inserção na Tabela de Staging
STAGING_COLUMN_ORDER = [
    "product_id", "product_name", "category_name", "price_cents", "shipping_cost_cents",
    "purchase_date", "seller_name", "purchase_location_code", "purchase_rating",
    "payment_type", "installments_quantity", "latitude", "longitude", "etl_load_timestamp"
]

# --- Funções Auxiliares ---
def check_env_vars() -> bool:
    """Verifica se todas as variáveis de ambiente essenciais estão definidas."""
    logger.debug("Verificando variáveis de ambiente essenciais...")
    required_vars = {
        "API_BASE_URL": API_BASE_URL,
        "MONGO_URI": MONGO_URI, "MONGO_DB_NAME": MONGO_DB_NAME, "MONGO_RAW_COLLECTION_NAME": MONGO_RAW_COLLECTION_NAME,
        "POSTGRES_HOST": POSTGRES_HOST, "POSTGRES_DB_NAME": POSTGRES_DB_NAME,
        "POSTGRES_USER": POSTGRES_USER, "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
        "POSTGRES_STAGING_TABLE_NAME": POSTGRES_STAGING_TABLE_NAME
    }
    missing_vars = [name for name, value in required_vars.items() if not value]
    if missing_vars:
        logger.error(f"Variáveis de ambiente ausentes: {', '.join(missing_vars)}. Encerrando.")
        return False
    logger.info("Todas as variáveis de ambiente essenciais estão presentes.")
    return True

def setup_initial_database_schema(pg_manager_instance: PostgresManager) -> bool:
    """Cria as tabelas do Data Warehouse (staging, dimensões, fato) se não existirem."""
    logger.info("--- Fase 0: Configurando Esquema do Banco de Dados PostgreSQL (se necessário) ---")
    try:
        if not os.path.isdir(SCHEMA_SCRIPTS_DIR):
            logger.error(f"Diretório de scripts de esquema não encontrado: {SCHEMA_SCRIPTS_DIR}")
            return False
        if not pg_manager_instance.setup_database_schema(SCHEMA_SCRIPTS_DIR):
            logger.error("Falha crítica ao configurar o esquema do banco de dados. Verifique os logs do PostgresManager.")
            return False
        logger.info("Esquema do banco de dados PostgreSQL verificado/configurado com sucesso.")
        return True
    except Exception as e_schema:
        logger.error(f"Erro crítico durante a configuração do esquema do banco de dados: {e_schema}", exc_info=True)
        return False

def populate_data_warehouse(pg_manager_instance: PostgresManager) -> bool:
    """Popula as tabelas de Dimensão e Fato a partir da Staging Table."""
    logger.info("--- Etapa 6: Populando Dimensões e Tabela Fato a partir da Staging ---")

    populate_scripts_ordered = [
        "01_populate_dim_tempo.sql",
        "02_populate_dim_local.sql",
        "03_populate_dim_vendedor.sql",
        "04_populate_dim_produto.sql",
        "05_populate_dim_pagamento.sql",
        "06_populate_fato_vendas.sql"
    ]

    if not os.path.isdir(POPULATE_DW_SCRIPTS_DIR):
        logger.error(f"Diretório de scripts de população do DW não encontrado: {POPULATE_DW_SCRIPTS_DIR}")
        logger.warning("Lógica de população do Data Warehouse (Etapa 6) pulada devido à ausência do diretório de scripts.")
        return True # Retorna True para não parar o pipeline se os scripts ainda não existem, mas avisa.

    all_scripts_succeeded = True
    for script_name in populate_scripts_ordered:
        script_path = os.path.join(POPULATE_DW_SCRIPTS_DIR, script_name)
        if not os.path.exists(script_path):
            logger.warning(f"Script de população do DW não encontrado: {script_path}. Pulando este script.")
            continue # Pula para o próximo script

        try:
            logger.info(f"Executando script de população do DW: {script_name}...")
            pg_manager_instance.execute_sql_file(script_path)
            logger.info(f"Script '{script_name}' executado com sucesso.")
        except Exception as e:
            logger.error(f"Falha ao executar script de população '{script_name}': {e}", exc_info=True)
            all_scripts_succeeded = False
            # Por segurança, parar no primeiro erro de população.
            break
    
    if all_scripts_succeeded:
        logger.info("Processo de população do Data Warehouse (Etapa 6) concluído com sucesso.")
    else:
        logger.error("Processo de população do Data Warehouse (Etapa 6) encontrou erros.")
    return all_scripts_succeeded


# --- Execução Principal do Pipeline ---
def run_pipeline():
    """
    Orquestra a execução de todas as fases do pipeline ETL.
    """
    logger.info("==================================================")
    logger.info(" Iniciando Pipeline ETL Completo")
    logger.info("==================================================")

    if not check_env_vars():
        return

    # Instâncias dos componentes do pipeline
    api_extractor = APIExtractor(url=API_BASE_URL)
    data_transformer = Transform()

    api_data = []
    raw_data_from_mongo = []
    transformed_data = []

    # Fase 0: Setup do Esquema do Banco de Dados PostgreSQL
    try:
        with PostgresManager(db_host=POSTGRES_HOST, db_port=POSTGRES_PORT, db_name=POSTGRES_DB_NAME,
                             db_user=POSTGRES_USER, db_password=POSTGRES_PASSWORD) as pg_setup_mgr:
            if not setup_initial_database_schema(pg_setup_mgr):
                logger.critical("Encerrando pipeline: Falha no setup do esquema do PostgreSQL.")
                return
    except Exception as e_pg_setup:
        logger.critical(f"Não foi possível conectar ao PostgreSQL para o setup do esquema: {e_pg_setup}", exc_info=True)
        return

    # Etapa 1: Extraindo dados da API
    try:
        logger.info("--- Etapa 1: Extraindo dados da API ---")
        api_data = api_extractor.get_data()
        if not api_data: logger.warning("Nenhum dado novo extraído da API.")
        else: logger.info(f"Extraídos {len(api_data)} registros da API.")
    except Exception as e:
        logger.error(f"Erro crítico na Etapa 1 (Extração API): {e}", exc_info=True)
        return

    # Etapa 2: Carregando dados brutos no MongoDB
    if api_data:
        try:
            logger.info(f"--- Etapa 2: Carregando dados brutos no MongoDB (DB: {MONGO_DB_NAME}, Coleção: {MONGO_RAW_COLLECTION_NAME}) ---")
            with MongoManager(mongo_uri=MONGO_URI, db_name=MONGO_DB_NAME) as mongo_load_mgr:
                if mongo_load_mgr.set_active_collection(MONGO_RAW_COLLECTION_NAME):
                    logger.info(f"Coleção ativa para carga/limpeza: {mongo_load_mgr.collection_name}")
                    if not mongo_load_mgr.delete_all_documents():
                        logger.error(f"Falha ao limpar a coleção '{MONGO_RAW_COLLECTION_NAME}'. Encerrando.")
                        return
                    if mongo_load_mgr.add_data(api_data):
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
        logger.info("Nenhum dado novo da API para carregar no MongoDB. As etapas de processamento de dados podem não ter novos dados.")

    # Etapa 3: Extraindo dados brutos do MongoDB para transformação
    try:
        logger.info(f"--- Etapa 3: Extraindo dados brutos do MongoDB (DB: {MONGO_DB_NAME}, Coleção: {MONGO_RAW_COLLECTION_NAME}) ---")
        with MongoManager(mongo_uri=MONGO_URI, db_name=MONGO_DB_NAME) as mongo_extract_mgr:
            raw_data_from_mongo = mongo_extract_mgr.extract_data(source_collection_name=MONGO_RAW_COLLECTION_NAME)
            if not raw_data_from_mongo:
                logger.warning(f"Nenhum dado bruto extraído da coleção '{MONGO_RAW_COLLECTION_NAME}' para transformação.")
            else:
                logger.info(f"{len(raw_data_from_mongo)} registros brutos extraídos do MongoDB.")
    except Exception as e:
        logger.error(f"Erro na Etapa 3 (Extração MongoDB): {e}", exc_info=True)
        return

    # Etapa 4: Transformando os Dados
    if raw_data_from_mongo:
        try:
            logger.info("--- Etapa 4: Transformando os dados ---")
            transformed_data = data_transformer.transform_data(raw_data_from_mongo)
            if not transformed_data:
                logger.warning("Nenhum dado retornado após a transformação.")
            else:
                logger.info(f"{len(transformed_data)} registros transformados com sucesso.")
        except Exception as e:
            logger.error(f"Erro crítico na Etapa 4 (Transformação): {e}", exc_info=True)
            return
    else:
        logger.info("Nenhum dado extraído do MongoDB para transformação.")

    # Etapa 5: Carregar Dados Transformados na Tabela de Staging do PostgreSQL
    if transformed_data:
        logger.info(f"--- Etapa 5: Carregando dados transformados na tabela de staging PostgreSQL ('{POSTGRES_STAGING_TABLE_NAME}') ---")
        try:
            with PostgresManager(db_host=POSTGRES_HOST, db_port=POSTGRES_PORT, db_name=POSTGRES_DB_NAME,
                                 db_user=POSTGRES_USER, db_password=POSTGRES_PASSWORD) as pg_staging_mgr:
                # A tabela de staging já foi criada na Fase 0. Apenas truncar e carregar.
                pg_staging_mgr.truncate_table(POSTGRES_STAGING_TABLE_NAME)
                logger.info(f"Tabela de staging '{POSTGRES_STAGING_TABLE_NAME}' truncada.")

                if pg_staging_mgr.load_data_to_staging(POSTGRES_STAGING_TABLE_NAME, transformed_data, STAGING_COLUMN_ORDER):
                    logger.info(f"Dados transformados carregados com sucesso na tabela de staging '{POSTGRES_STAGING_TABLE_NAME}'.")
                else:
                    logger.error(f"Falha ao carregar dados transformados na tabela de staging '{POSTGRES_STAGING_TABLE_NAME}'. Encerrando.")
                    return
        except (psycopg2.Error, ConnectionError, ValueError) as db_err:
            logger.error(f"Erro de banco de dados/conexão/configuração na Etapa 5 (Carga Staging): {db_err}", exc_info=True)
            return
        except Exception as e:
            logger.error(f"Erro inesperado na Etapa 5 (Carga Staging): {e}", exc_info=True)
            return
    else:
        logger.info("Nenhum dado transformado disponível para carregar na tabela de staging.")

    # Etapa 6: Popular Dimensões e Fato a partir da Staging
    # Só executa se dados foram carregados na staging com sucesso
    if transformed_data: # Verificar se houve dados para transformar e, portanto, para carregar na staging
        logger.info("Iniciando Etapa 6: População do Data Warehouse.")
        try:
            with PostgresManager(db_host=POSTGRES_HOST, db_port=POSTGRES_PORT, db_name=POSTGRES_DB_NAME,
                                 db_user=POSTGRES_USER, db_password=POSTGRES_PASSWORD) as pg_dw_mgr:
                if not populate_data_warehouse(pg_dw_mgr):
                    logger.error("Falha ao popular o Data Warehouse a partir da staging. Pipeline concluído com erros na Etapa 6.")
                else:
                    logger.info("Data Warehouse populado/atualizado com sucesso a partir da staging.")
        except Exception as e:
            logger.error(f"Erro crítico na Etapa 6 (População DW): {e}", exc_info=True)
    else:
        logger.info("Nenhum dado carregado na staging, pulando Etapa 6 (População DW).")


    logger.info("==================================================")
    logger.info(" Pipeline ETL Completo Finalizado.")
    logger.info("==================================================")

if __name__ == "__main__":
    run_pipeline()