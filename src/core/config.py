
# --- bibliotecas
import logging
import os
import sys # para logging
from dotenv import load_dotenv
from typing import List, Optional # type hint em STAGING_COLUMN_ORDER

# --- configuração inicial de Logging --- será aplicada assim que este módulo for importado e garante que o logging seja configurado desde o inicio.
LOG_LEVEL_STR: str = os.getenv('LOG_LEVEL', 'INFO').upper()
LOG_LEVEL_INT: int = getattr(logging, LOG_LEVEL_STR, logging.INFO)
LOG_FORMAT: str = os.getenv('LOG_FORMAT', '%(asctime)s - %(levelname)s - [%(name)s:%(module)s:%(funcName)s:%(lineno)d] - %(message)s')
LOG_DATE_FORMAT: str = '%Y-%m-%d %H:%M:%S'

# limpar handlers existentes do logger raiz para evitar duplicação e garantir que config seja usada
root_logger = logging.getLogger()
if root_logger.hasHandlers():
    root_logger.handlers.clear()

root_logger.setLevel(LOG_LEVEL_INT)

console_handler = logging.StreamHandler(sys.stdout) # log para a saída padrão
console_handler.setLevel(LOG_LEVEL_INT)
formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)
console_handler.setFormatter(formatter)
root_logger.addHandler(console_handler)

config_logger = logging.getLogger(__name__) # logger para config

# --- carregar variáveis de ambiente do .env 
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), '.env') # .env na raiz do projeto
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
    config_logger.info(f"Variáveis de ambiente carregadas de: {dotenv_path}")
else:
    config_logger.warning(f"Arquivo .env não encontrado em {dotenv_path}. Usando variáveis de ambiente do sistema ou defaults.")

# --- configurações globais do pipeline
config_logger.debug("Lendo configurações do pipeline do ambiente...")

# API
API_BASE_URL: Optional[str] = os.getenv('API_BASE_URL')

# MongoDB
MONGO_URI: Optional[str] = os.getenv('MONGO_URI')
MONGO_DB_NAME: Optional[str] = os.getenv('MONGO_DB')
MONGO_RAW_COLLECTION_NAME: Optional[str] = os.getenv('MONGO_RAW_COLLECTION')

# PostgreSQL
POSTGRES_HOST: Optional[str] = os.getenv('POSTGRES_HOST')
POSTGRES_PORT: Optional[str] = os.getenv('POSTGRES_PORT') 
POSTGRES_DB_NAME: Optional[str] = os.getenv('POSTGRES_DB')
POSTGRES_USER: Optional[str] = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD: Optional[str] = os.getenv('POSTGRES_PASSWORD')
POSTGRES_STAGING_TABLE_NAME: str = os.getenv('POSTGRES_STAGING_TABLE', 'staging_produtos_ecommerce')

# caminhos para os diretórios com os scripts SQL
# __file__ é o caminho para src/core/config.py
# os.path.dirname(__file__) é src/core/
# os.path.dirname(os.path.dirname(__file__)) é src/ (sobe um nível do core)
SRC_DIR: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SCHEMA_SCRIPTS_DIR: str = os.path.join(SRC_DIR, "sql", "schema")
POPULATE_DW_SCRIPTS_DIR: str = os.path.join(SRC_DIR, "sql", "populate")

# ordem das colunas para insert staging
STAGING_COLUMN_ORDER: List[str] = [
    "product_id", "product_name", "category_name", "price_cents", "shipping_cost_cents",
    "purchase_date", "seller_name", "purchase_location_code", "purchase_rating",
    "payment_type", "installments_quantity", "latitude", "longitude", "etl_load_timestamp"
]

config_logger.info("Módulo de configuração 'config.py' carregado e inicializado.")

# --- função para verificar variáveis essenciais
def get_critical_env_var(var_name: str) -> str:
    """Obtém uma variável de ambiente crítica, levantando um erro se não estiver definida."""
    value = os.getenv(var_name)
    if value is None:
        msg = f"Variável de ambiente crítica '{var_name}' não está definida."
        config_logger.error(msg)
        raise ValueError(msg)
    return value