"""
Módulo responsável pela extração de dados da API.
Este módulo contém funcionalidades para acessar e extrair
dados provenientes da fonte API e MongoDB.
"""

# --- bibliotecas
import requests
import os
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
import logging
from mongodb import MongoDB, MONGO_URI, MONGO_DB, MONGO_RAW_COLLECTION

# --- configs
load_dotenv()
API_BASE_URL = os.getenv('API_BASE_URL')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger('api_extractor')

# --- classes
class APIExtractor:
    """
    Classe que extrai os dados da API.
    
    Esta classe é responsável por fazer requisições HTTP a uma API,
    extrair os dados e retorná-los em formato adequado para processamento.
    """
    
    def __init__(self, url=None, timeout=10):
        """
        Inicializa o extrator de API.
        
        Args:
            url: URL da API (opcional, usa API_BASE_URL do .env se não fornecido)
            timeout: Tempo limite para a requisição em segundos
        """
        self.url = url or API_BASE_URL
        self.timeout = timeout
        
        if self.url is None:
            raise ValueError('URL da API não informada')
            
        self.data = []

    def __extract(self) -> Optional[List[Dict[str, Any]]]:
        """
        Extrai dados da API definida na inicialização da classe.
        
        Returns:
            Lista de dicionários com os dados da API ou None em caso de erro.
        """
        logger.info('Iniciando extração dos dados da API')
        try:
            response = requests.get(self.url, timeout=self.timeout)
            response.raise_for_status()
            dados = response.json()
            logger.info(f'Coleta de dados bem sucedida: {response.status_code}')
            logger.info(f'Tamanho da coleta: {len(dados)}')
            return dados
        except requests.exceptions.HTTPError as err:
            logger.error(f"Erro HTTP: {err}")
        except requests.exceptions.ConnectionError as err:
            logger.error(f"Erro de conexão com o servidor: {err}")
        except requests.exceptions.Timeout as err:
            logger.error(f"Timeout na requisição: {err}")
        except requests.exceptions.JSONDecodeError as err:
            logger.error(f"Erro ao decodificar JSON: {err}")
        except requests.exceptions.RequestException as err:
            logger.error(f"Erro na requisição: {err}")
        except Exception as err:
            logger.error(f"Erro inesperado: {err}")
        return None

    def get_data(self, force_refresh=False) -> List[Dict[str, Any]]:
        """
        Obtém os dados da API com validação.
        
        Args:
            force_refresh: Se True, força uma nova extração mesmo que já existam dados
            
        Returns:
            Lista de dicionários com os dados ou lista vazia em caso de erro.
        """
        if self.data and not force_refresh: # tem dados e não precisa atualizar?
            return self.data
            
        data = self.__extract()

        if data is None:
            logger.warning('Retornando lista vazia pois não foi possível extrair os dados')
            return []
            
        self.data = data
        return self.data

class MongoDBExtractor:
    """
    Classe que extrai os dados brutos armazenados no MongoDB.
    
    Esta classe é responsável por extrair os dados do MongoDB
    utilizando a classe MongoDB para manipulação do banco de dados.
    """
    
    def __init__(self, mongo_uri=MONGO_URI, db_name=MONGO_DB, collection_name=MONGO_RAW_COLLECTION):
        """
        Inicializa o extrator de dados do MongoDB.
        
        Args:
            mongo_uri: URI de conexão com o MongoDB
            db_name: Nome do banco de dados
            collection_name: Nome da coleção que contém os dados brutos
        """
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.collection_name = collection_name
        
    def extract_data(self, query=None, limit=None, sort=None) -> List[Dict[str, Any]]:
        """
        Extrai dados brutos do MongoDB.
        
        Args:
            query: Filtro para a consulta (opcional)
            limit: Número máximo de documentos a retornar (opcional)
            sort: Critério de ordenação (opcional)
            
        Returns:
            Lista de documentos extraídos do MongoDB
        """
        logger.info(f"Iniciando extração de dados usando MongoDBExtractor")
        
        # instancia MongoDB para extrair os dados
        mongo_db = MongoDB(
            mongo_uri=self.mongo_uri,
            db_name=self.db_name,
            collection_name=self.collection_name
        )
        
        dados = mongo_db.extract_data(query=query, limit=limit, sort=sort)
        
        logger.info(f"MongoDBExtractor concluiu a extração de {len(dados)} documentos")
        return dados
    
# --- testes

if __name__ == '__main__':
    extractor = APIExtractor()
    dados = extractor.get_data()
    if dados:
        print(dados[0])