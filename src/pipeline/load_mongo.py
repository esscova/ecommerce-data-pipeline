"""
Módulo para operações de carga (load) de dados no MongoDB.
Este módulo é responsável por todas as operações relacionadas ao carregamento
de dados no banco de dados MongoDB.
"""

import logging
import pymongo
import os
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

# Carrega variáveis de ambiente
load_dotenv()
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB = os.getenv('MONGO_DB', 'analytics')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'vendas')

# Configuração de logging
logger = logging.getLogger('mongodb_loader')

class MongoDBLoader:
    """
    Classe responsável pelo carregamento de dados no MongoDB.
    
    Esta classe encapsula todas as operações relacionadas à conexão
    e inserção de dados no banco de dados MongoDB.
    """
    
    def __init__(self, uri=None, db_name=None, collection_name=None):
        """
        Inicializa o loader do MongoDB.
        
        Args:
            uri: URI de conexão com o MongoDB (opcional, usa MONGO_URI do .env por padrão)
            db_name: Nome do banco de dados (opcional, usa MONGO_DB do .env por padrão)
            collection_name: Nome da coleção (opcional, usa MONGO_COLLECTION do .env por padrão)
        """
        self.uri = uri or MONGO_URI
        self.db_name = db_name or MONGO_DB
        self.collection_name = collection_name or MONGO_COLLECTION
        self.mongo_client = None
        
    def conectar(self) -> bool:
        """
        Estabelece uma conexão com o banco de dados MongoDB.
        
        Returns:
            True se a conexão foi bem sucedida, False caso contrário
        """
        if not self.uri:
            logger.error("URI de conexão do MongoDB não configurada")
            return False
            
        try:
            self.mongo_client = pymongo.MongoClient(self.uri)
            self.mongo_client.admin.command('ping')
            logger.info("Conexão com MongoDB estabelecida com sucesso")
            return True
        except pymongo.errors.ConnectionFailure as e:
            logger.error(f"Falha ao conectar com MongoDB: {e}")
            return False
        except Exception as e:
            logger.error(f"Erro ao conectar com MongoDB: {e}")
            return False
    
    def desconectar(self):
        """
        Fecha a conexão com o MongoDB se estiver aberta.
        """
        if self.mongo_client:
            self.mongo_client.close()
            logger.info("Conexão com MongoDB fechada")
    
    def carregar_dados(self, dados: List[Dict[str, Any]]) -> bool:
        """
        Carrega os dados no MongoDB.
        
        Args:
            dados: Lista de documentos (dicionários) a serem inseridos no MongoDB
            
        Returns:
            True se o carregamento foi bem sucedido, False caso contrário
        """
        if not dados:
            logger.warning("Nenhum dado para carregar no MongoDB")
            return False
            
        if not self.mongo_client and not self.conectar(): # sem conexão
            return False
            
        try:
            db = self.mongo_client[self.db_name]
            collection = db[self.collection_name]
            
            result = collection.insert_many(dados)
            
            logger.info(f"Carga concluída. {len(result.inserted_ids)} documentos inseridos no MongoDB.")
            return True
            
        except pymongo.errors.BulkWriteError as e:
            logger.error(f"Erro durante inserção em massa no MongoDB: {e}")
            return False
        except Exception as e:
            logger.error(f"Erro ao carregar dados no MongoDB: {e}")
            return False
        finally:
            self.desconectar()