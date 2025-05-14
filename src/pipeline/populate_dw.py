
# --- bibliotecas
import logging
import os

# --- módulos internos
from core.postgres_manager import PostgresManager 
from core import config 

# --- configuração de logging
logger = logging.getLogger(__name__)

def populate_data_warehouse_tables() -> bool:
    """
    popula as tabelas de dimensão e fato a partir da staging table,
    usando uma nova instância do postgresmanager.
    retorna true se bem-sucedido, false caso contrário.
    """
    logger.info("--- etapa 6: populando dimensões e tabela fato a partir da staging ---")

    populate_scripts_ordered = [
        "01_populate_dim_tempo.sql", "02_populate_dim_local.sql", "03_populate_dim_vendedor.sql",
        "04_populate_dim_produto.sql", "05_populate_dim_pagamento.sql", "06_populate_fato_vendas.sql"
    ]

    if not os.path.isdir(config.POPULATE_DW_SCRIPTS_DIR):
        logger.warning(f"diretório de scripts de população do dw não encontrado: {config.POPULATE_DW_SCRIPTS_DIR}. etapa 6 pulada.")
        return True # não é um erro crítico para o pipeline principal se os scripts ainda não existem

    all_scripts_succeeded = True
    try:
        with PostgresManager(db_host=config.POSTGRES_HOST,
                             db_port=config.POSTGRES_PORT,
                             db_name=config.POSTGRES_DB_NAME,
                             db_user=config.POSTGRES_USER,
                             db_password=config.POSTGRES_PASSWORD) as pg_dw_mgr:
            for script_name in populate_scripts_ordered:
                script_path = os.path.join(config.POPULATE_DW_SCRIPTS_DIR, script_name)
                if not os.path.exists(script_path):
                    logger.warning(f"script de população do dw não encontrado: {script_path}. pulando este script.")
                    continue

                try:
                    logger.info(f"executando script de população do dw: {script_name}...")
                    pg_dw_mgr.execute_sql_file(script_path)
                    logger.info(f"script '{script_name}' executado com sucesso.")
                except Exception as e: # erro ao executar um script sql específico
                    logger.error(f"falha ao executar script de população '{script_name}': {e}", exc_info=True)
                    all_scripts_succeeded = False
                    break # parar no primeiro erro de população
    except Exception as e_conn: # erro ao conectar ao pg para esta etapa
        logger.error(f"erro de conexão/configuração ao tentar popular o dw: {e_conn}", exc_info=True)
        return False
    
    if all_scripts_succeeded:
        logger.info("processo de população do data warehouse (etapa 6) concluído com sucesso.")
    else:
        logger.error("processo de população do data warehouse (etapa 6) encontrou erros.")
    return all_scripts_succeeded