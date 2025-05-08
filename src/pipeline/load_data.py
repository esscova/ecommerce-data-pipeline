"""
Módulo para operações de carga (load) de dados em diferentes destinos.
Este módulo é responsável por todas as operações relacionadas ao carregamento
e salvamento de dados em diferentes formatos e destinos, como arquivos JSON e MongoDB.
"""

# --- bibliotecas
import logging
import json
import os
import pymongo
from typing import List, Dict, Any
from dotenv import load_dotenv
from pathlib import Path

# --- variaveis
load_dotenv()
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB = os.getenv('MONGO_DB', 'analytics')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'vendas')

# --- função para resolver caminhos relativos à raiz do projeto
def get_project_root():
    """
    Retorna o caminho raiz do projeto independentemente de onde o script é executado.
    """
    # raiz do projeto
    current_path = Path(os.path.dirname(os.path.abspath(__file__)))
    
    # busca ate encontrar Makefile ou requirements.txt ... que estao na raiz
    while current_path != current_path.parent:
        if (current_path / "Makefile").exists() or (current_path / "requirements.txt").exists():
            return current_path
        if (current_path / "src").exists() and (current_path / "src").is_dir():
            return current_path
        current_path = current_path.parent
        
    # se não encontrar usa dois níveis acima como fallback 
    return Path(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# --- configs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('data_loader')

# --- classe
class LoadData:
    """
    Classe responsável pelo carregamento e salvamento de dados em diferentes destinos.
    
    Esta classe encapsula todas as operações relacionadas ao salvamento de dados
    em arquivos JSON e carregamento de dados no MongoDB.
    """
    
    def __init__(self, json_raw_path=None, json_transformed_path=None, 
                 mongo_uri=None, mongo_db=None, mongo_collection=None):
        """
        Inicializa o carregador de dados.
        
        Args:
            json_raw_path: Caminho para salvamento de dados brutos em JSON (opcional)
            json_transformed_path: Caminho para salvamento de dados transformados em JSON (opcional)
            mongo_uri: URI de conexão com o MongoDB (opcional, usa MONGO_URI do .env por padrão)
            mongo_db: Nome do banco de dados MongoDB (opcional, usa MONGO_DB do .env por padrão)
            mongo_collection: Nome da coleção MongoDB (opcional, usa MONGO_COLLECTION do .env por padrão)
        """
        # caminho raiz do projeto
        self.project_root = get_project_root()
        
        # caminhos padrão relativos à raiz do projeto
        default_raw_path = os.path.join(self.project_root, "src", "data", "raw", "vendas.json")
        default_transformed_path = os.path.join(self.project_root, "src", "data", "transformed", "vendas_transformadas.json")
        
        # caminhos fornecidos ou os padrões
        self.json_raw_path = json_raw_path or default_raw_path
        self.json_transformed_path = json_transformed_path or default_transformed_path
        
        self.mongo_uri = mongo_uri or MONGO_URI
        self.mongo_db = mongo_db or MONGO_DB
        self.mongo_collection = mongo_collection or MONGO_COLLECTION
        self.mongo_client = None
        
        logger.info(f"Caminho para dados brutos: {self.json_raw_path}")
        logger.info(f"Caminho para dados transformados: {self.json_transformed_path}")
        
    def save_raw_data(self, data: List[Dict[str, Any]], file_path: str = None, pretty=True) -> bool:
        """
        Salva dados brutos em um arquivo JSON.
        
        Args:
            data: Dados a serem salvos
            file_path: Caminho completo onde o arquivo será salvo (sobrescreve json_raw_path se fornecido)
            pretty: Se True, formata o JSON com indentação
            
        Returns:
            True se o arquivo foi salvo com sucesso, False caso contrário
        """
        path = file_path or self.json_raw_path
        if not path:
            logger.error("Caminho para dados brutos não especificado")
            return False
            
        return self._save_to_json(data, path, pretty)
    
    def save_transformed_data(self, data: List[Dict[str, Any]], file_path: str = None, pretty=True) -> bool:
        """
        Salva dados transformados em um arquivo JSON.
        
        Args:
            data: Dados a serem salvos
            file_path: Caminho completo onde o arquivo será salvo (sobrescreve json_transformed_path se fornecido)
            pretty: Se True, formata o JSON com indentação
            
        Returns:
            True se o arquivo foi salvo com sucesso, False caso contrário
        """
        path = file_path or self.json_transformed_path
        if not path:
            logger.error("Caminho para dados transformados não especificado")
            return False
            
        return self._save_to_json(data, path, pretty)
        
    def _save_to_json(self, data: List[Dict[str, Any]], file_path: str, pretty=True) -> bool:
        """
        Salva dados em um arquivo JSON.
        
        Args:
            data: Dados a serem salvos
            file_path: Caminho completo onde o arquivo será salvo
            pretty: Se True, formata o JSON com indentação
            
        Returns:
            True se o arquivo foi salvo com sucesso, False caso contrário
        """
        if not data: 
            logger.error("Não há dados para salvar em JSON")
            return False
        
        if not os.path.isabs(file_path): # se nao for absoluto
            file_path = os.path.join(self.project_root, file_path)
            
        logger.info(f"Salvando dados no caminho absoluto: {file_path}")
        
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                indent = 4 if pretty else None
                json.dump(data, f, indent=indent, ensure_ascii=False)
            logger.info(f"Dados salvos com sucesso em {file_path}")
            return True
        except Exception as err:
            logger.error(f"Erro ao salvar arquivo JSON: {err}")
            return False
    
    def _conectar_mongodb(self) -> bool:
        """
        Estabelece uma conexão com o banco de dados MongoDB.
        
        Returns:
            True se a conexão foi bem sucedida, False caso contrário
        """
        if not self.mongo_uri:
            logger.error("URI de conexão do MongoDB não configurada")
            return False
            
        try:
            self.mongo_client = pymongo.MongoClient(self.mongo_uri)
            self.mongo_client.admin.command('ping')
            logger.info("Conexão com MongoDB estabelecida com sucesso")
            return True
        except pymongo.errors.ConnectionFailure as e:
            logger.error(f"Falha ao conectar com MongoDB: {e}")
            return False
        except Exception as e:
            logger.error(f"Erro ao conectar com MongoDB: {e}")
            return False
    
    def _desconectar_mongodb(self):
        """
        Fecha a conexão com o MongoDB se estiver aberta.
        """
        if self.mongo_client:
            self.mongo_client.close()
            logger.info("Conexão com MongoDB fechada")
    
    def carregar_no_mongodb(self, dados: List[Dict[str, Any]], 
                         db_name=None, collection_name=None) -> bool:
        """
        Carrega os dados no MongoDB.
        
        Args:
            dados: Lista de documentos (dicionários) a serem inseridos no MongoDB
            db_name: Nome do banco de dados (opcional, usa o valor definido no construtor)
            collection_name: Nome da coleção (opcional, usa o valor definido no construtor)
            
        Returns:
            True se o carregamento foi bem sucedido, False caso contrário
        """
        if not dados:
            logger.warning("Nenhum dado para carregar no MongoDB")
            return False
            
        db_name = db_name or self.mongo_db
        collection_name = collection_name or self.mongo_collection
            
        # [ conectar no mongo se ainda não estiver conectado ]
        if not self.mongo_client and not self._conectar_mongodb():
            return False
            
        try:
            db = self.mongo_client[db_name]
            collection = db[collection_name]
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
            self._desconectar_mongodb()