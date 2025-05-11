"""
Orquestrador do pipeline.
"""


# --- bibliotecas
import logging
import os
from dotenv import load_dotenv

from pipeline.api_data_extractor import APIExtractor
from pipeline.transform_data import Transform
from db.mongo_manager import MongoManager

# --- configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s -[%(name)s:%(funcName)s:%(lineno)d]- %(message)s'
)

logger = logging.getLogger(__name__)

# --- carregar variáveis de ambiente
load_dotenv()
logger.info('Variáveis de ambiente carregadas.')

# --- variáveis padrão
API_BASE_URL = os.getenv('API_BASE_URL')
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB = os.getenv('MONGO_DB')
MONGO_RAW_COLLECTION = os.getenv('MONGO_RAW_COLLECTION')


# --- execução do pipeline
def main():
    """
    Função principal que executa o pipeline ETL.
    """
    logger.info("==================================================")
    logger.info(" Iniciando Pipeline")
    logger.info("==================================================")

    if not API_BASE_URL:
        logger.error("Variável de ambiente 'API_BASE_URL' não definida.")
        return

    if not MONGO_URI:
        logger.error("Variável de ambiente 'MONGO_URI' não definida.")
        return

    if not MONGO_DB:
        logger.error("Variável de ambiente 'MONGO_DB' não definida.")
        return

    if not MONGO_RAW_COLLECTION:
        logger.error("Variável de ambiente 'MONGO_RAW_COLLECTION' não definida.")
        return

    api_extractor = APIExtractor(API_BASE_URL)
    data_transformer = Transform()
    api_data = []
    transformed_data = []

    try:
        logger.info("--- Etapa 1: Extraindo dados da API ---")
        api_data = api_extractor.get_data()

        if not api_data:
            logger.error("Nenhum dado extraido da API.")
        else:
            logger.info(f"Quantidade de dados extraidos da API: {len(api_data)}")

    except Exception as e:
        logger.error(f"Erro ao executar pipeline: {e}", exc_info=True)
        logger.info("Pipeline encerrado devido a erro ao extrair dados da API.")
        return

    if api_data:
        try:
            logger.info("--- Etapa 2: Carregando dados brutos no MongoDB ---") 
            with MongoManager(MONGO_URI, MONGO_DB) as mongo_manager:
                if mongo_manager.set_active_collection(MONGO_RAW_COLLECTION):
                    logger.info(f"Coleção ativa definida para: {mongo_manager.db_name}/{mongo_manager.collection_name}")
                    mongo_manager.add_data(api_data)
                else:
                    logger.error(f"Erro ao definir coleção ativa para: {mongo_manager.db_name}/{mongo_manager.collection_name}")

            logger.info("--- Etapa 3: Transformando dados ---")
            transformed_data = data_transformer.transform_data(api_data)

            if not transformed_data:
                logger.warning("Nenhum dado transformado.")
            else:   
                 logger.info(f"{len(transformed_data)} registros transformados com sucesso.")               

        except ConnectionError as ce:
            logger.error(f"Erro ao conectar ao MongoDB: {ce}", exc_info=True)
            logger.info("Pipeline encerrado devido a erro ao conectar ao MongoDB.")
            return
        except Exception as e:
            logger.error(f"Erro ao executar pipeline: {e}", exc_info=True)
            logger.info("Pipeline encerrado devido a erro ao carregar os dados brutos no MongoDB.")
            return
    else:
        logger.info("Nenhum dado extraido da API. Pipeline encerrado.")       

if __name__ == "__main__":
    main()