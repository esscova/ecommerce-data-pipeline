"""
Este módulo é responsável por todas interações com o banco de dados MongoDB.
"""

# --- bibliotecas
from pymongo import MongoClient, errors
import logging
import os
from dotenv import load_dotenv
from typing import List, Dict, Any

# --- configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger('mongodb')

# --- carregar variáveis de ambiente
load_dotenv()

# --- variáveis padrão
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
MONGO_DB = os.getenv('MONGO_DB', 'analytics')
MONGO_RAW_COLLECTION = os.getenv('MONGO_RAW_COLLECTION', 'raw_data')
MONGO_TRANSFORMED_COLLECTION = os.getenv('MONGO_TRANSFORMED_COLLECTION', 'transformed_data')

class MongoDB:
    def __init__(self, mongo_uri=MONGO_URI, db_name=MONGO_DB, collection_name=MONGO_RAW_COLLECTION):
        """
        Inicializa a conexão com o MongoDB.
        
        Args:
            mongo_uri: URI de conexão com o MongoDB
            db_name: Nome do banco de dados
            collection_name: Nome da coleção
        """
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.client = None
        self.db = None
        self.collection = None
        
        if not self.mongo_uri:
            raise ValueError("URI de conexão do MongoDB não configurada")
    
    def connect(self) -> bool:
        """
        Estabelece uma conexão com o banco de dados MongoDB.
        
        Returns:
            True se a conexão foi bem sucedida, False caso contrário
        """
        try:
            self.client = MongoClient(self.mongo_uri)
            self.client.admin.command('ping')  
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]
            logger.info("Conexão com MongoDB estabelecida com sucesso")
            return True
        except errors.ConnectionFailure as e:
            logger.error(f"Falha ao conectar com MongoDB: {e}")
            return False
        except Exception as e:
            logger.error(f"Erro ao conectar com MongoDB: {e}")
            return False
    
    def disconnect(self):
        """
        Fecha a conexão com o MongoDB se estiver aberta.
        """
        if self.client:
            self.client.close()
            logger.info("Conexão com MongoDB fechada")
            self.client = None
            self.db = None
            self.collection = None

    def load_data(self, data: List[Dict[str, Any]]) -> bool:
        """
        Carrega dados para o MongoDB.
        
        Args:
            data: Lista de dicionários com os dados a serem inseridos
            
        Returns:
            True se a operação foi bem sucedida, False caso contrário
        """
        if not self.client and not self.connect():
            return False
            
        try:
            result = self.collection.insert_many(data)
            logger.info(f"Dados carregados com sucesso: {len(result.inserted_ids)} documentos inseridos")
            return True
        except Exception as e:
            logger.error(f"Erro ao carregar dados no MongoDB: {e}")
            return False
        finally:
            self.disconnect()

    def extract_data(self, query=None, limit=None, sort=None) -> List[Dict[str, Any]]:
        """
        Extrai dados do MongoDB.
        
        Args:
            query: Filtro para a consulta (opcional)
            limit: Número máximo de documentos a retornar (opcional)
            sort: Critério de ordenação (opcional)
            
        Returns:
            Lista de documentos extraídos do MongoDB
        """
        logger.info(f"Iniciando extração de dados do MongoDB - Coleção: {self.collection_name}")
        
        if not self.client and not self.connect():
            return []
        
        try:
            # se nenhuma query for fornecida -> retorna tudo
            if query is None: 
                query = {}
                
            # se nenhuma ordenação for fornecida -> ordena por _id decrescente
            if sort is None: 
                sort = [("_id", -1)]  # -1 para DESCENDING
            
            # executa a consulta
            cursor = self.collection.find(query).sort(sort)
            if limit:
                cursor = cursor.limit(limit)
            
            # converte para lista
            dados = list(cursor)
            
            logger.info(f"Extração concluída. {len(dados)} documentos extraídos do MongoDB")
            return dados
            
        except Exception as e:
            logger.error(f"Erro ao extrair dados do MongoDB: {e}")
            return []
        finally:
            self.disconnect()

# --- testes
if __name__ == "__main__":
    from extract_data import APIExtractor
    data = APIExtractor().get_data()
    if data:
        print('Carregando dados no MongoDB...')
        if MongoDB().load_data(data):
            print('Dados carregados com sucesso')
        else:
            print('Falha ao carregar dados no MongoDB')
    else:
        print('Falha ao obter dados da API')
        
    print('Extraindo dados do MongoDB...')
    print(MongoDB().extract_data(limit=2))
