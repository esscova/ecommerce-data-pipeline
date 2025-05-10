"""
Módulo para extração de dados de uma API de produtos.

Este módulo contém a classe APIExtractor, projetada para realizar
requisições HTTP a uma API especificada, coletar os dados
e retorná-los em um formato de lista de dicionários.
É utilizável com a instrução 'with', embora não gerencie
recursos externos que exijam fechamento explícito.
"""
import requests
import os
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
import logging

load_dotenv()

logger = logging.getLogger(__name__)

class APIExtractor:
    def __init__(self, url: Optional[str] = None, timeout: int = 10):
        self.url = url or os.getenv('API_BASE_URL')
        self.timeout = timeout
        self.data: List[Dict[str, Any]] = []

        if not self.url:
            logger.error("URL da API não informada (API_BASE_URL não configurada ou url não passada).")
            raise ValueError('URL da API não informada. Configure API_BASE_URL no .env ou passe a url diretamente.')

    def __extract(self) -> Optional[List[Dict[str, Any]]]:
        logger.info(f'Iniciando extração dos dados da API: {self.url}')
        try:
            response = requests.get(self.url, timeout=self.timeout)
            response.raise_for_status()
            dados = response.json()
            logger.info(f'Coleta de dados da API bem sucedida: {response.status_code}')
            if isinstance(dados, list):
                logger.info(f'Número de registros coletados da API: {len(dados)}')
            else:
                logger.info('Coleta de dados da API retornou um objeto único, não uma lista.')
            return dados
        except requests.exceptions.HTTPError as err:
            logger.error(f"Erro HTTP ao acessar {self.url}: {err}")
        except requests.exceptions.ConnectionError as err:
            logger.error(f"Erro de conexão com o servidor {self.url}: {err}")
        except requests.exceptions.Timeout as err:
            logger.error(f"Timeout na requisição para {self.url}: {err}")
        except requests.exceptions.JSONDecodeError as err:
            logger.error(f"Erro ao decodificar JSON da resposta de {self.url}: {err}")
        except requests.exceptions.RequestException as err:
            logger.error(f"Erro na requisição para {self.url}: {err}")
        except Exception as err:
            logger.error(f"Erro inesperado durante a extração da API {self.url}: {err}")
        return None

    def get_data(self, force_refresh: bool = False) -> List[Dict[str, Any]]:
        if self.data and not force_refresh:
            logger.info("Retornando dados da API do cache interno da instância.")
            return self.data

        extracted_data = self.__extract()

        if extracted_data is None:
            logger.warning('Retornando lista vazia pois não foi possível extrair os dados da API.')
            self.data = []
            return []

        if not isinstance(extracted_data, list):
            logger.warning(f"Dados da API ({self.url}) não são uma lista, mas sim {type(extracted_data)}. Envolvendo em uma lista se for um dict.")
            self.data = [extracted_data] if isinstance(extracted_data, dict) else []
            if not self.data and isinstance(extracted_data, dict) is False:
                 logger.error(f"Formato de dados da API ({self.url}) inesperado: {type(extracted_data)}. Esperava-se uma lista ou um dicionário.")
        else:
            self.data = extracted_data
        return self.data

    def __enter__(self):
        logger.debug(f"APIExtractor entrando no contexto para URL: {self.url}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug(f"APIExtractor saindo do contexto para URL: {self.url}")
        return False

    def __repr__(self):
        return f"APIExtractor(url={self.url}, timeout={self.timeout})"