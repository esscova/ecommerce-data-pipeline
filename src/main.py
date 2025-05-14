"""
orquestrador do pipeline ETL completo.

fases:
0. setup do esquema do banco de dados postgresql.
1. coleta de dados da API.
2. armazenamento de dados brutos no MongoDB.
3. extração dos dados brutos do MongoDB.
4. transformação dos dados extraídos.
5. carga dos dados transformados na tabela de staging do postgresql.
6. populacão das tabelas de dimensão e fato a partir da staging.
"""

# --- bibliotecas ---
import logging
import os
import psycopg2

# --- módulos da aplicação ---
from core import config
from core.mongo_manager import MongoManager
from core.postgres_manager import PostgresManager # necessário para a etapa de staging
from pipeline.api_data_extractor import APIExtractor
from pipeline.transform_data import Transform
from pipeline.setup_database import setup_initial_postgres_schema
from pipeline.populate_dw import populate_data_warehouse_tables

# --- configuração de logging ---
logger = logging.getLogger(__name__) # o logging já foi configurado por core.config

# --- funções auxiliares ---
def check_critical_configurations() -> bool:
    """verifica se as configurações críticas do pipeline estão definidas."""
    logger.debug("verificando configurações críticas do pipeline...")
    critical_configs = {
        "API_BASE_URL": config.API_BASE_URL,
        "MONGO_URI": config.MONGO_URI, "MONGO_DB_NAME": config.MONGO_DB_NAME,
        "MONGO_RAW_COLLECTION_NAME": config.MONGO_RAW_COLLECTION_NAME,
        "POSTGRES_HOST": config.POSTGRES_HOST, "POSTGRES_DB_NAME": config.POSTGRES_DB_NAME,
        "POSTGRES_USER": config.POSTGRES_USER, "POSTGRES_PASSWORD": config.POSTGRES_PASSWORD,
        "POSTGRES_STAGING_TABLE_NAME": config.POSTGRES_STAGING_TABLE_NAME,
        "SCHEMA_SCRIPTS_DIR": config.SCHEMA_SCRIPTS_DIR,
        "POPULATE_DW_SCRIPTS_DIR": config.POPULATE_DW_SCRIPTS_DIR
    }
    missing_configs = [name for name, value in critical_configs.items() if not value]
    if missing_configs:
        logger.error(f"configurações críticas ausentes: {', '.join(missing_configs)}. encerrando.")
        return False

    if not os.path.isdir(config.SCHEMA_SCRIPTS_DIR):
        logger.error(f"diretório de scripts de esquema não encontrado: {config.SCHEMA_SCRIPTS_DIR}. encerrando.")
        return False
    if not os.path.isdir(config.POPULATE_DW_SCRIPTS_DIR): # agora é um aviso dentro de populate_dw.py
        logger.warning(f"diretório de scripts de população do dw não encontrado: {config.POPULATE_DW_SCRIPTS_DIR}.")

    logger.info("todas as configurações críticas e diretórios de esquema verificados.")
    return True

# --- execução principal do pipeline ---
def run_pipeline():
    """
    orquestra a execução de todas as fases do pipeline etl.
    """
    logger.info("==================================================")
    logger.info(" iniciando pipeline etl completo")
    logger.info("==================================================")

    if not check_critical_configurations():
        return

    # instâncias dos componentes principais do etl
    api_extractor = APIExtractor(url=config.API_BASE_URL)
    data_transformer = Transform()

    # variáveis para armazenar dados entre etapas
    api_data = []
    raw_data_from_mongo = []
    transformed_data = []

    # fase 0: setup do esquema do banco de dados postgresql
    # a função setup_initial_postgres_schema lida com sua própria instância de postgresmanager
    if not setup_initial_postgres_schema():
        logger.critical("encerrando pipeline: falha no setup do esquema do postgresql.")
        return

    # etapa 1: extraindo dados da api
    try:
        logger.info("--- etapa 1: extraindo dados da api ---")
        api_data = api_extractor.get_data()
        if not api_data: logger.warning("nenhum dado novo extraído da api.")
        else: logger.info(f"extraídos {len(api_data)} registros da api.")
    except Exception as e:
        logger.error(f"erro crítico na etapa 1 (extração api): {e}", exc_info=True)
        return

    # etapa 2: carregando dados brutos no mongodb
    if api_data:
        try:
            logger.info(f"--- etapa 2: carregando dados brutos no mongodb (db: {config.MONGO_DB_NAME}, coleção: {config.MONGO_RAW_COLLECTION_NAME}) ---")
            with MongoManager(mongo_uri=config.MONGO_URI, db_name=config.MONGO_DB_NAME) as mongo_load_mgr:
                if mongo_load_mgr.set_active_collection(config.MONGO_RAW_COLLECTION_NAME):
                    logger.info(f"coleção ativa para carga/limpeza: {mongo_load_mgr.collection_name}")
                    if not mongo_load_mgr.delete_all_documents():
                        logger.error(f"falha ao limpar a coleção '{config.MONGO_RAW_COLLECTION_NAME}'. encerrando.")
                        return
                    if mongo_load_mgr.add_data(api_data):
                        logger.info(f"dados brutos carregados com sucesso na '{config.MONGO_RAW_COLLECTION_NAME}'.")
                    else:
                        logger.error(f"falha ao adicionar dados brutos à '{config.MONGO_RAW_COLLECTION_NAME}'. encerrando.")
                        return
                else:
                    logger.error(f"falha ao definir coleção ativa '{config.MONGO_RAW_COLLECTION_NAME}'. encerrando.")
                    return
        except Exception as e:
            logger.error(f"erro na etapa 2 (carga mongodb): {e}", exc_info=True)
            return
    else:
        logger.info("nenhum dado novo da api para carregar no mongodb.")

    # etapa 3: extraindo dados brutos do mongodb para transformação
    try:
        logger.info(f"--- etapa 3: extraindo dados brutos do mongodb (db: {config.MONGO_DB_NAME}, coleção: {config.MONGO_RAW_COLLECTION_NAME}) ---")
        with MongoManager(mongo_uri=config.MONGO_URI, db_name=config.MONGO_DB_NAME) as mongo_extract_mgr:
            raw_data_from_mongo = mongo_extract_mgr.extract_data(source_collection_name=config.MONGO_RAW_COLLECTION_NAME)
            if not raw_data_from_mongo:
                logger.warning(f"nenhum dado bruto extraído da '{config.MONGO_RAW_COLLECTION_NAME}' para transformação.")
            else:
                logger.info(f"{len(raw_data_from_mongo)} registros brutos extraídos do mongodb.")
    except Exception as e:
        logger.error(f"erro na etapa 3 (extração mongodb): {e}", exc_info=True)
        return

    # etapa 4: transformando os dados
    if raw_data_from_mongo:
        try:
            logger.info("--- etapa 4: transformando os dados ---")
            transformed_data = data_transformer.transform_data(raw_data_from_mongo)
            if not transformed_data:
                logger.warning("nenhum dado retornado após a transformação.")
            else:
                logger.info(f"{len(transformed_data)} registros transformados com sucesso.")
        except Exception as e:
            logger.error(f"erro crítico na etapa 4 (transformação): {e}", exc_info=True)
            return
    else:
        logger.info("nenhum dado extraído do mongodb para transformação.")

    # etapa 5: carregar dados transformados na tabela de staging do postgresql
    if transformed_data:
        logger.info(f"--- etapa 5: carregando dados transformados na tabela de staging postgresql ('{config.POSTGRES_STAGING_TABLE_NAME}') ---")
        try:
            with PostgresManager(db_host=config.POSTGRES_HOST, db_port=config.POSTGRES_PORT,
                                 db_name=config.POSTGRES_DB_NAME, db_user=config.POSTGRES_USER,
                                 db_password=config.POSTGRES_PASSWORD) as pg_staging_mgr:
                pg_staging_mgr.truncate_table(config.POSTGRES_STAGING_TABLE_NAME)
                logger.info(f"tabela de staging '{config.POSTGRES_STAGING_TABLE_NAME}' truncada.")

                if pg_staging_mgr.load_data_to_staging(config.POSTGRES_STAGING_TABLE_NAME,
                                                       transformed_data, config.STAGING_COLUMN_ORDER):
                    logger.info(f"dados transformados carregados com sucesso na staging '{config.POSTGRES_STAGING_TABLE_NAME}'.")
                else:
                    logger.error(f"falha ao carregar dados transformados na staging '{config.POSTGRES_STAGING_TABLE_NAME}'. encerrando.")
                    return
        except (psycopg2.Error, ConnectionError, ValueError) as db_err:
            logger.error(f"erro de banco de dados/conexão/configuração na etapa 5 (carga staging): {db_err}", exc_info=True)
            return
        except Exception as e:
            logger.error(f"erro inesperado na etapa 5 (carga staging): {e}", exc_info=True)
            return
    else:
        logger.info("nenhum dado transformado disponível para carregar na tabela de staging.")

    # etapa 6: popular dimensões e fato a partir da staging
    if transformed_data: # só tenta popular o dw se dados foram carregados na staging
        logger.info("iniciando etapa 6: população do data warehouse.")
        # a função populate_data_warehouse_tables tb lida com sua própria instância de postgresmanager
        if not populate_data_warehouse_tables():
            logger.error("falha ao popular o data warehouse a partir da staging. pipeline concluído com erros na etapa 6.")
        else:
            logger.info("data warehouse populado/atualizado com sucesso a partir da staging.")
    else:
        logger.info("nenhum dado carregado na staging, pulando etapa 6 (população dw).")

    logger.info("==================================================")
    logger.info(" pipeline etl completo finalizado.")
    logger.info("==================================================")

if __name__ == "__main__":
    run_pipeline()