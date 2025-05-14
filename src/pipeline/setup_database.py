
# --- bibliotecas
import logging
import os

# --- módulos internos
from core.postgres_manager import PostgresManager 
from core import config 

# --- configuração de logging
logger = logging.getLogger(__name__)

def setup_initial_postgres_schema() -> bool:
    """
    cria as tabelas do data warehouse (staging, dimensões, fato) se não existirem,
    usando uma nova instância do postgresmanager.
    retorna true se bem-sucedido, false caso contrário.
    """
    logger.info("--- fase 0: configurando esquema do banco de dados postgresql (se necessário) ---")
    try:
        # cria uma nova instância do postgresmanager especificamente para esta tarefa
        with PostgresManager(db_host=config.POSTGRES_HOST,
                             db_port=config.POSTGRES_PORT,
                             db_name=config.POSTGRES_DB_NAME,
                             db_user=config.POSTGRES_USER,
                             db_password=config.POSTGRES_PASSWORD) as pg_setup_mgr:
            
            if not os.path.isdir(config.SCHEMA_SCRIPTS_DIR):
                logger.error(f"diretório de scripts de esquema não encontrado: {config.SCHEMA_SCRIPTS_DIR}")
                return False
            
            if not pg_setup_mgr.setup_database_schema(config.SCHEMA_SCRIPTS_DIR):
                logger.error("falha crítica ao configurar o esquema do banco de dados. verifique os logs do postgresmanager.")
                return False
            
            logger.info("esquema do banco de dados postgresql verificado/configurado com sucesso.")
            return True
            
    except Exception as e_schema:
        logger.error(f"erro crítico durante a configuração do esquema do banco de dados: {e_schema}", exc_info=True)
        return False
