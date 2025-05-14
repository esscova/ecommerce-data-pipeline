"""
módulo para extração de dados de uma api de produtos.

este módulo contém a classe APIExtractor, projetada para realizar
requisições http a uma api especificada, coletar os dados
e retorná-los em um formato de lista de dicionários.
é utilizável com a instrução 'with', embora não gerencie
recursos externos que exijam fechamento explícito.
"""

# --- bibliotecas
import requests
from typing import List, Dict, Any, Optional
import logging

# --- modulo interno
from core import config

# --- configuração de logging do módulo
logger = logging.getLogger(__name__)

class APIExtractor:
    def __init__(self, url: Optional[str] = None, timeout: int = 10):
        """
        inicializa o api extractor.

        args:
            url (optional[str]): a url da api. se none, usa config.api_base_url.
            timeout (int): timeout para a requisição em segundos.
        """
        # usa a url passada ou a url do arquivo de configuração
        self.url = url or config.API_BASE_URL
        self.timeout = timeout
        self.data: List[Dict[str, Any]] = [] # cache interno para os dados

        if not self.url:
            msg = "url da api não informada e não configurada em config.api_base_url."
            logger.error(msg)
            raise ValueError(msg)
        logger.debug(f"APIExtractor inicializado para URL: {self.url}")

    def __extract(self) -> Optional[List[Dict[str, Any]]]:
        """
        método privado para realizar a requisição http e extrair os dados.
        """
        logger.info(f"iniciando extração dos dados da api: {self.url}")
        try:
            response = requests.get(self.url, timeout=self.timeout)
            response.raise_for_status() # levanta httpError para status ruins (4xx ou 5xx)
            dados = response.json()
            logger.info(f"coleta de dados da api bem sucedida: status {response.status_code}")
            if isinstance(dados, list):
                logger.info(f"número de registros coletados da api: {len(dados)}")
            else:
                logger.info(f"coleta de dados da api retornou um objeto único (tipo: {type(dados)}), não uma lista.")
            return dados
        except requests.exceptions.HTTPError as err:
            logger.error(f"erro http ao acessar {self.url}: {err}")
        except requests.exceptions.ConnectionError as err:
            logger.error(f"erro de conexão com o servidor {self.url}: {err}")
        except requests.exceptions.Timeout as err:
            logger.error(f"timeout na requisição para {self.url}: {err}")
        except requests.exceptions.JSONDecodeError as err:
            logger.error(f"erro ao decodificar json da resposta de {self.url}: {err}")
        except requests.exceptions.RequestException as err: # erro genérico do requests
            logger.error(f"erro na requisição para {self.url}: {err}")
        except Exception as err: # erro inesperado não previsto
            logger.error(f"erro inesperado durante a extração da api {self.url}: {err}", exc_info=True)
        return None

    def get_data(self, force_refresh: bool = False) -> List[Dict[str, Any]]:
        """
        obtém os dados da api. usa um cache interno a menos que force_refresh seja true.

        args:
            force_refresh (bool): se true, ignora o cache e faz uma nova requisição.

        returns:
            list[dict[str, any]]: lista de dicionários com os dados, ou lista vazia em caso de erro.
        """
        if self.data and not force_refresh:
            logger.info("retornando dados da api do cache interno da instância.")
            return self.data

        logger.debug("cache da api ignorado ou vazio, realizando nova extração.")
        extracted_data = self.__extract()

        if extracted_data is None:
            logger.warning("nenhum dado extraído da api, retornando lista vazia.")
            self.data = [] # reseta o cache em caso de falha
            return []

        # normaliza a saída para sempre ser uma lista de dicionários
        if not isinstance(extracted_data, list):
            if isinstance(extracted_data, dict):
                logger.info(f"dados da api ({self.url}) não são uma lista, mas um dict. envolvendo em uma lista.")
                self.data = [extracted_data]
            else:
                logger.error(f"formato de dados da api ({self.url}) inesperado: {type(extracted_data)}. esperava-se lista ou dict. retornando lista vazia.")
                self.data = []
        else:
            self.data = extracted_data
        
        logger.debug(f"dados da api armazenados no cache interno. tamanho: {len(self.data)}")
        return self.data

    def __enter__(self):
        """suporte para o protocolo de gerenciador de contexto (instrução 'with')."""
        logger.debug(f"APIExtractor entrando no contexto para url: {self.url}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """suporte para o protocolo de gerenciador de contexto (instrução 'with')."""
        logger.debug(f"APIExtractor saindo do contexto para url: {self.url}")
        # não há recursos externos para liberar, então apenas retorna false para não suprimir exceções.
        return False

    def __repr__(self) -> str:
        """representação string oficial do objeto."""
        return f"{self.__class__.__name__}(url='{self.url}', timeout={self.timeout})"