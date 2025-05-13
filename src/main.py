"""
Orquestrador do pipeline.

Fases:
1. Coleta de dados da API.
2. Armazenamento de dados brutos no MongoDB.
3. Extração dos dados brutos do MongoDB.
4. Transformação dos dados extraídos.
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
    level=os.getenv('LOG_LEVEL', 'INFO').upper(),
    format='%(asctime)s - %(levelname)s - [%(name)s:%(funcName)s:%(lineno)d] - %(message)s'
)

logger = logging.getLogger(__name__)

# --- carregar variáveis de ambiente
load_dotenv()
logger.info('Variáveis de ambiente carregadas.')

# --- variáveis padrão
API_BASE_URL = os.getenv('API_BASE_URL')
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB_NAME = os.getenv('MONGO_DB') 
MONGO_RAW_COLLECTION_NAME = os.getenv('MONGO_RAW_COLLECTION') 

# --- execução do pipeline
def main():
    """
    Função principal que executa o pipeline ETL.
    """
    logger.info("==================================================")
    logger.info(" Iniciando Pipeline - Fases: Coleta, Carga Bruta, Extração Bruta, Transformação")
    logger.info("==================================================")

    # validar variaveis de ambiente
    if not API_BASE_URL:
        logger.error("Variável de ambiente 'API_BASE_URL' não definida. Encerrando.")
        return
    if not MONGO_URI:
        logger.error("Variável de ambiente 'MONGO_URI' não definida. Encerrando.")
        return
    if not MONGO_DB_NAME:
        logger.error("Variável de ambiente 'MONGO_DB' não definida. Encerrando.")
        return
    if not MONGO_RAW_COLLECTION_NAME:
        logger.error("Variável de ambiente 'MONGO_RAW_COLLECTION' não definida. Encerrando.")
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

        if not api_data:
            logger.warning("Nenhum dado extraído da API. O pipeline pode não ter dados para processar nas etapas subsequentes.")
            
        else:
            logger.info(f"Quantidade de dados extraídos da API: {len(api_data)}")

    except Exception as e:
        logger.error(f"Erro crítico ao extrair dados da API: {e}", exc_info=True)
        logger.info("Pipeline encerrado devido a erro na extração de dados da API.")
        return

    # Etapa 2: Carregando dados brutos no MongoDB
    if api_data: # dados da api?
        try:
            logger.info(f"--- Etapa 2: Carregando dados brutos no MongoDB (DB: {MONGO_DB_NAME}, Coleção: {MONGO_RAW_COLLECTION_NAME}) ---")
            with MongoManager(mongo_uri=MONGO_URI, db_name=MONGO_DB_NAME) as mongo_manager_load:
                if mongo_manager_load.set_active_collection(MONGO_RAW_COLLECTION_NAME):
                    logger.info(f"Coleção ativa para operações de carga/limpeza definida para: {mongo_manager_load.db_name}/{mongo_manager_load.collection_name}")

                    # limpar coleção ANTES de adicionar novos dados
                    logger.info(f"Limpando a coleção '{MONGO_RAW_COLLECTION_NAME}' antes da nova carga...")
                    if not mongo_manager_load.delete_all_documents(): # Chama delete na coleção ativa
                        logger.error(f"Falha ao limpar a coleção '{MONGO_RAW_COLLECTION_NAME}'. A carga prosseguirá, mas pode resultar em dados inconsistentes ou duplicados.")
                        logger.info("Pipeline encerrado devido à falha na limpeza da coleção de dados brutos.")
                        return
                    else:
                        logger.info(f"Coleção '{MONGO_RAW_COLLECTION_NAME}' limpa com sucesso.")

                    if mongo_manager_load.add_data(api_data): # add_data usará a coleção ativa
                        logger.info(f"Dados brutos carregados com sucesso na coleção '{MONGO_RAW_COLLECTION_NAME}'.")
                    else:
                        logger.error(f"Falha ao adicionar dados brutos à coleção '{MONGO_RAW_COLLECTION_NAME}' (método add_data retornou False).")
                        logger.info("Pipeline encerrado devido à falha na carga de dados brutos.")
                        return # se carga falhar
                else:
                    logger.error(f"Falha ao definir a coleção ativa '{MONGO_RAW_COLLECTION_NAME}' para carga/limpeza. Não foi possível prosseguir.")
                    logger.info("Pipeline encerrado.")
                    return

        except ConnectionError as ce:
            logger.error(f"Erro de conexão ao MongoDB durante a carga de dados brutos: {ce}", exc_info=True)
            logger.info("Pipeline encerrado.")
            return
        except Exception as e:
            logger.error(f"Erro ao carregar dados brutos no MongoDB: {e}", exc_info=True)
            logger.info("Pipeline encerrado.")
            return
    else:
        logger.info("Nenhum dado extraído da API para carregar no MongoDB. As etapas subsequentes que dependem desses dados serão puladas.")


    # Etapa 3: Extraindo dados brutos do MongoDB para transformação

    if api_data: # valida novamente
        try:
            logger.info(f"--- Etapa 3: Extraindo dados brutos do MongoDB para transformação (DB: {MONGO_DB_NAME}, Coleção: {MONGO_RAW_COLLECTION_NAME}) ---")
            with MongoManager(mongo_uri=MONGO_URI, db_name=MONGO_DB_NAME) as mongo_manager_extract:
                raw_data_from_mongo = mongo_manager_extract.extract_data(source_collection_name=MONGO_RAW_COLLECTION_NAME)

                if not raw_data_from_mongo:
                    logger.warning(f"Nenhum dado bruto extraído da coleção '{MONGO_RAW_COLLECTION_NAME}' do MongoDB para transformação.")
                else:
                    logger.info(f"{len(raw_data_from_mongo)} registros brutos extraídos do MongoDB para transformação.")

        except ConnectionError as ce:
            logger.error(f"Erro de conexão ao MongoDB durante a extração de dados brutos: {ce}", exc_info=True)
            logger.info("Pipeline encerrado.")
            return
        except Exception as e:
            logger.error(f"Erro ao extrair dados brutos do MongoDB: {e}", exc_info=True)
            logger.info("Pipeline encerrado.")
            return

    # Etapa 4: Transformando os Dados
    if raw_data_from_mongo: # tem dados brutos do mongo?
        try:
            logger.info("--- Etapa 4: Transformando os dados ---")
            transformed_data = data_transformer.transform_data(raw_data_from_mongo)

            if not transformed_data:
                logger.warning("Nenhum dado retornado após a transformação. Verifique a lógica de transformação ou os dados de entrada.")
            else:
                logger.info(f"{len(transformed_data)} registros transformados com sucesso.")
        except Exception as e:
            logger.error(f"Erro crítico durante a transformação dos dados: {e}", exc_info=True)
            logger.info("Pipeline encerrado devido a erro na transformação.")
            return
    elif api_data and not raw_data_from_mongo: # tem api mas nao tem raw no mongo?
        logger.warning("Havia dados da API, mas nenhum dado foi extraído do MongoDB para transformação.")
    else: 
        logger.info("Nenhum dado disponível para transformação.")


    # 5. Carregar Dados Transformados no PostgreSQL

    if transformed_data:
        logger.info(f"--- Etapa 5: Carregar dados transformados no PostgreSQL --- (Pendente)")
        pass
    else:
        logger.info("Nenhum dado transformado disponível para as próximas etapas de carga.")

    logger.info("==================================================")
    logger.info(" Pipeline (Coleta, Carga Bruta, Extração Bruta, Transformação) Concluído")
    logger.info("==================================================")

if __name__ == "__main__":
    main()