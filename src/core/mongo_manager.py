"""
módulo para gerenciamento de conexões e operações com o mongodb.

este módulo fornece a classe mongomanager, que encapsula a lógica
de conexão, operações de extração e carga de dados, e o gerenciamento
de contexto para interações com o mongodb.
"""

# --- bibliotecas ---
from typing import List, Dict, Any, Optional
from pymongo import MongoClient, errors as pymongo_errors
from pymongo.database import Database # para type hint
from pymongo.collection import Collection # para type hint
import logging

# --- modulo interno ---
from . import config 

# --- configuração de logging do módulo
logger = logging.getLogger(__name__)

# --- classe ---
class MongoManager:
    """
    gerenciador de conexões e operações para o mongodb.
    """
    def __init__(self,
                 mongo_uri: Optional[str] = None,
                 db_name: Optional[str] = None,
                 collection_name: Optional[str] = None, # coleção padrão para a instância
                 server_selection_timeout_ms: int = 5000):

        # usa os valores passados ou busca das configurações centralizadas
        self.mongo_uri: str = mongo_uri or config.MONGO_URI
        self.db_name: str = db_name or config.MONGO_DB_NAME
        self.collection_name: Optional[str] = collection_name
        self.server_selection_timeout_ms: int = server_selection_timeout_ms

        self.client: Optional[MongoClient] = None
        self.db: Optional[Database] = None
        self.collection: Optional[Collection] = None # coleção ativa/padrão da instância
        self.is_connected: bool = False

        if not self.mongo_uri:
            msg = "uri de conexão do mongodb não fornecida nem configurada."
            logger.error(msg)
            raise ValueError(msg)
        if not self.db_name:
            msg = "nome do banco de dados mongodb não fornecido nem configurado."
            logger.error(msg)
            raise ValueError(msg)
        
        logger.debug(f"MongoManager instanciado para DB: {self.db_name}, Coleção Padrão: {self.collection_name or 'Nenhuma'}")


    def connect(self) -> bool:
        if self.is_connected and self.client:
            logger.debug(f"já conectado ao mongodb: {self.db_name}/{self.collection_name or 'n/a'}")
            return True
        try:
            logger.debug(f"tentando conectar ao mongodb: {self.mongo_uri} (db: {self.db_name})")
            self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=self.server_selection_timeout_ms)
            self.client.admin.command('ping')
            self.db = self.client[self.db_name]
            
            collection_log_name = "n/a (padrão não definida no init)"
            if self.collection_name:
                self.collection = self.db[self.collection_name]
                collection_log_name = self.collection_name
            
            logger.info(f"conexão com mongodb estabelecida: {self.db_name}/{collection_log_name}")
            self.is_connected = True
            return True
        
        except pymongo_errors.ConnectionFailure as e:
            logger.error(f"falha de conexão com mongodb ({self.mongo_uri}, db: {self.db_name}): {e}")
            self._reset_connection_state()
            return False
        except Exception as e: 
            logger.error(f"erro inesperado ao conectar com mongodb ({self.mongo_uri}, db: {self.db_name}): {e}", exc_info=True)
            self._reset_connection_state()
            return False

    def _reset_connection_state(self):
        """método auxiliar para resetar o estado da conexão."""
        self.client = None
        self.db = None
        self.collection = None # reseta a coleção ativa da instância também
        self.is_connected = False


    def disconnect(self) -> None:
        if self.client:
            try:
                self.client.close()
                logger.info(f"conexão com mongodb fechada: {self.db_name}/{self.collection_name or 'n/a'}")
            except Exception as e:
                logger.error(f"erro ao fechar a conexão com mongodb: {e}", exc_info=True)
        self._reset_connection_state()


    def __enter__(self):
        if self.connect():
            return self
        # connect() já loga o erro detalhado
        raise ConnectionError(f"falha ao estabelecer conexão com mongodb ({self.db_name}) no __enter__.")

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        return False # não suprime exceções

    def set_active_collection(self, collection_name: str) -> bool:
        """define ou altera a coleção ativa para operações subsequentes nesta instância."""
        if not self.is_connected or self.db is None:
            logger.error("não é possível definir a coleção ativa: não conectado ao mongodb ou banco de dados não definido.")
            return False
        
        try:
            self.collection = self.db[collection_name]
            self.collection_name = collection_name # atualiza o nome da coleção ativa da instância
            logger.info(f"coleção ativa alterada para: {self.db_name}/{self.collection_name}")
            return True
        except Exception as e:
            logger.error(f"erro ao tentar definir coleção ativa '{collection_name}': {e}", exc_info=True)
            return False


    def _get_collection_to_operate(self, operation_collection_name: Optional[str], operation_type: str) -> Optional[Collection]:
        """método auxiliar para determinar em qual coleção operar."""
        if not self.is_connected or self.db is None:
            logger.error(f"não conectado ao mongodb para {operation_type}. operação cancelada.")
            return None

        collection_to_operate = None
        log_collection_name = ""

        if operation_collection_name:
            collection_to_operate = self.db[operation_collection_name]
            log_collection_name = operation_collection_name
            logger.debug(f"{operation_type} na coleção específica: {self.db_name}/{log_collection_name}")
        elif self.collection is not None: # usa a coleção ativa da instância se definida
            collection_to_operate = self.collection
            log_collection_name = self.collection_name
            logger.debug(f"{operation_type} na coleção ativa da instância: {self.db_name}/{log_collection_name}")
        else:
            logger.error(f"nenhuma coleção de destino (específica ou ativa) definida para {operation_type}.")
            return None
        return collection_to_operate


    def add_data(self,
                 data: List[Dict[str, Any]],
                 target_collection_name: Optional[str] = None) -> bool:
        """adiciona uma lista de documentos a uma coleção."""
        collection_to_use = self._get_collection_to_operate(target_collection_name, "add_data")
        if not collection_to_use:
            return False

        if not data:
            logger.info(f"nenhum dado fornecido para adicionar na coleção '{collection_to_use.name}'.")
            return True

        try:
            result = collection_to_use.insert_many(data, ordered=False) # ordered=False pode ser mais rápido se a ordem não importa
            logger.info(f"{len(result.inserted_ids)} documentos inseridos com sucesso na coleção '{collection_to_use.name}'.")
            return True
        except pymongo_errors.BulkWriteError as bwe:
            logger.error(f"erro de escrita em lote ao adicionar dados na coleção '{collection_to_use.name}': {bwe.details}", exc_info=True)
            # bwe.details pode conter informações sobre quais documentos falharam
            return False
        except Exception as e:
            logger.error(f"erro ao adicionar dados na coleção '{collection_to_use.name}' do mongodb: {e}", exc_info=True)
            return False

    def extract_data(self,
                     query: Optional[Dict[str, Any]] = None,
                     projection: Optional[Dict[str, Any]] = None,
                     limit: Optional[int] = None,
                     sort: Optional[List[tuple]] = None,
                     source_collection_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """extrai documentos de uma coleção com base nos critérios fornecidos."""
        collection_to_use = self._get_collection_to_operate(source_collection_name, "extract_data")
        if not collection_to_use:
            return []

        logger.debug(f"extraindo dados da coleção '{collection_to_use.name}': query: {query}, projection: {projection}, limit: {limit}, sort: {sort}")
        
        try:
            processed_query = query if query is not None else {}
            cursor = collection_to_use.find(processed_query, projection)

            if sort:
                cursor = cursor.sort(sort)
            
            if limit is not None and limit > 0:
                cursor = cursor.limit(limit)

            documents = list(cursor)
            logger.info(f"extração da coleção '{collection_to_use.name}' concluída: {len(documents)} documentos encontrados.")
            return documents
        except Exception as e:
            logger.error(f"erro ao extrair dados da coleção '{collection_to_use.name}' do mongodb: {e}", exc_info=True)
            return []
    
    def delete_all_documents(self, target_collection_name: Optional[str] = None) -> bool:
        """deleta todos os documentos de uma coleção especificada ou da coleção ativa."""
        collection_to_clear = self._get_collection_to_operate(target_collection_name, "delete_all_documents")
        if not collection_to_clear:
            return False

        try:
            logger.warning(f"deletando todos os documentos da coleção '{collection_to_clear.name}' no banco '{self.db_name}'...")
            result = collection_to_clear.delete_many({})
            logger.info(f"{result.deleted_count} documentos deletados com sucesso da coleção '{collection_to_clear.name}'.")
            return True
        except Exception as e:
            logger.error(f"erro ao deletar documentos da coleção '{collection_to_clear.name}': {e}", exc_info=True)
            return False