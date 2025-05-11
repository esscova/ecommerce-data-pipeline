"""
Módulo para gerenciamento de conexões e operações com o MongoDB.

Este módulo fornece a classe MongoManager, que encapsula a lógica
de conexão, operações de extração e carga de dados, e o gerenciamento
de contexto para interações com o MongoDB.
"""

# --- bibliotecas
import os
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
from pymongo import MongoClient, errors as pymongo_errors
import logging

# --- configs
load_dotenv()
logging.basicConfig(
    level="INFO",
    format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MongoManager:
    """
    Gerenciador de conexões e operações para o MongoDB.
    """
    def __init__(self,
                 mongo_uri: Optional[str] = None,
                 db_name: Optional[str] = None,
                 collection_name: Optional[str] = None,
                 server_selection_timeout_ms: int = 5000):

        self.mongo_uri: str = mongo_uri or os.getenv('MONGO_URI', 'mongodb://localhost:27017')
        self.db_name: str = db_name or os.getenv('MONGO_DB', 'mydatabase')
        self.collection_name: Optional[str] = collection_name 
        self.server_selection_timeout_ms: int = server_selection_timeout_ms

        self.client: Optional[MongoClient] = None
        self.db = None
        self.collection = None 
        self.is_connected: bool = False

        if not self.mongo_uri:
            logger.error("URI de conexão do MongoDB (MONGO_URI) não configurada.")
            raise ValueError("URI de conexão do MongoDB não configurada.")
        if not self.db_name:
            logger.error("Nome do banco de dados MongoDB (MONGO_DB) não configurado.")
            raise ValueError("Nome do banco de dados MongoDB não configurado.")

    def connect(self) -> bool:
        if self.is_connected and self.client:
            logger.debug(f"Já conectado ao MongoDB: {self.db_name}/{self.collection_name or 'N/A'}")
            return True
        try:
            logger.debug(f"Tentando conectar ao MongoDB: {self.mongo_uri} (DB: {self.db_name})")
            self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=self.server_selection_timeout_ms)
            self.client.admin.command('ping') 
            self.db = self.client[self.db_name]
            
            if self.collection_name: # tem coleção definida?
                self.collection = self.db[self.collection_name]
                logger.info(f"Conexão com MongoDB estabelecida: {self.db_name}/{self.collection_name}")
            else:
                logger.info(f"Conexão com MongoDB estabelecida: {self.db_name} (coleção padrão não definida na inicialização)")
            self.is_connected = True
            return True
        
        except pymongo_errors.ConnectionFailure as e:
            logger.error(f"Falha de conexão com MongoDB ({self.mongo_uri}, DB: {self.db_name}): {e}")
            self.client = None
            self.is_connected = False
            return False
        except Exception as e: 
            logger.error(f"Erro inesperado ao conectar com MongoDB ({self.mongo_uri}, DB: {self.db_name}): {e}")
            self.client = None
            self.is_connected = False
            return False

    def disconnect(self) -> None:
        if self.client: # conectado?
            self.client.close()
            logger.info(f"Conexão com MongoDB fechada: {self.db_name}/{self.collection_name or 'N/A'}")
        # resetar
        self.client = None 
        self.db = None
        self.collection = None
        self.is_connected = False

    def __enter__(self):
        if self.connect():
            return self
        raise ConnectionError(f"Falha ao estabelecer conexão com MongoDB ({self.db_name}) no __enter__.")

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        return False 

    def set_active_collection(self, collection_name: str) -> bool:
        """Define ou altera a coleção ativa para operações subsequentes."""
        
        if not self.is_connected or self.db is None: # conectado e banco de dados definido?
            logger.error("Não é possível definir a coleção ativa: não conectado ao MongoDB ou banco de dados não definido.")
            return False
        
        self.collection_name = collection_name
        self.collection = self.db[self.collection_name]
        logger.info(f"Coleção ativa alterada para: {self.db_name}/{self.collection_name}")
        
        return True

    def add_data(self,
                 data: List[Dict[str, Any]],
                 target_collection_name: Optional[str] = None) -> bool:
        """Adiciona uma lista de documentos a uma coleção."""

        if not self.is_connected or self.db is None: # conectado ou banco de dados definido?
            logger.error("Não conectado ao MongoDB. Não é possível adicionar dados.")
            return False

        collection_to_use = None

        if target_collection_name: # coleção?
            collection_to_use = self.db[target_collection_name]
            logger.debug(f"Adicionando dados na coleção específica: {self.db_name}/{target_collection_name}")
        
        elif self.collection is not None: # coleção padrão?
            collection_to_use = self.collection
            logger.debug(f"Adicionando dados na coleção padrão da instância: {self.db_name}/{self.collection_name}")
        
        else:
            logger.error("Nenhuma coleção de destino especificada ou definida para adicionar os dados.")
            return False

        if not data: # sem dados?
            logger.info(f"Nenhum dado fornecido para adicionar na coleção '{collection_to_use.name}'.")
            return True # considera sucesso pois não há o que fazer

        try:
            result = collection_to_use.insert_many(data)
            logger.info(f"{len(result.inserted_ids)} documentos inseridos com sucesso na coleção '{collection_to_use.name}'.")
            return True
        except pymongo_errors.BulkWriteError as bwe:
            logger.error(f"Erro de escrita em lote ao adicionar dados na coleção '{collection_to_use.name}': {bwe.details}")
            return False
        except Exception as e:
            logger.error(f"Erro ao adicionar dados na coleção '{collection_to_use.name}' do MongoDB: {e}")
            return False

    def extract_data(self,
                     query: Optional[Dict[str, Any]] = None,
                     projection: Optional[Dict[str, Any]] = None,
                     limit: Optional[int] = None,
                     sort: Optional[List[tuple]] = None,
                     source_collection_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Extrai documentos de uma coleção com base nos critérios fornecidos."""

        if not self.is_connected or not self.db: # conectado ou banco de dados definido?
            logger.error("Não conectado ao MongoDB. Não é possível extrair dados.")
            return []

        collection_to_use = None

        if source_collection_name: # coleção?
            collection_to_use = self.db[source_collection_name]
            logger.debug(f"Extraindo dados da coleção específica: {self.db_name}/{source_collection_name}")
        
        elif self.collection is not None: # coleção padrão?
            collection_to_use = self.collection
            logger.debug(f"Extraindo dados da coleção padrão da instância: {self.db_name}/{self.collection_name}")
        
        else:
            logger.error("Nenhuma coleção de origem especificada ou definida para extrair os dados.")
            return []

        logger.debug(f"Extraindo dados da coleção '{collection_to_use.name}': Query: {query}, Projection: {projection}, Limit: {limit}, Sort: {sort}")
        
        try:
            processed_query = query if query is not None else {}
            cursor = collection_to_use.find(processed_query, projection)

            if sort: # ordenar?
                cursor = cursor.sort(sort)
            
            if limit is not None and limit > 0: # limitar?
                cursor = cursor.limit(limit)

            documents = list(cursor) # casting cursor -> list
            logger.info(f"Extração da coleção '{collection_to_use.name}' concluída: {len(documents)} documentos encontrados.")
            return documents
        
        except Exception as e:
            logger.error(f"Erro ao extrair dados da coleção '{collection_to_use.name}' do MongoDB: {e}")
            return []