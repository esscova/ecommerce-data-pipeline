#
# Classe que extrai os dados da API e os salva em um arquivo JSON
#

# --- bibliotecas
import requests
import os
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
import logging
import json

# --- configs
load_dotenv()
API_BASE_URL = os.getenv('API_BASE_URL')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- classe
class APIExtractor:
    def __init__(self, url=None, timeout=10, file_path='src/data/dados.json'):
        """
        Inicializa o extrator de API.
        
        Args:
            url: URL da API (opcional, usa API_BASE_URL do .env se não fornecido)
            timeout: Tempo limite para a requisição em segundos
            file_path: Caminho para salvar os dados por padrão
        """
        self.url = url or API_BASE_URL
        self.timeout = timeout
        self.file_path = file_path
        
        if self.url is None:
            raise ValueError('URL da API não informada')
            
        self.data = []

    def __extract(self) -> Optional[List[Dict[str, Any]]]:
        """
        Extrai dados da API definida na inicialização da classe.
        
        Returns:
            Lista de dicionários com os dados da API ou None em caso de erro.
        """
        logging.info('\nIniciando extração dos dados da API')
        try:
            response = requests.get(self.url, timeout=self.timeout)
            response.raise_for_status()
            dados = response.json()
            logging.info(f'Coleta de dados bem sucedida: {response.status_code}')
            logging.info(f'Tamanho da coleta: {len(dados)}')
            return dados
        except requests.exceptions.HTTPError as err:
            logging.error(f"Erro HTTP: {err}")
        except requests.exceptions.ConnectionError as err:
            logging.error(f"Erro de conexão com o servidor: {err}")
        except requests.exceptions.Timeout as err:
            logging.error(f"Timeout na requisição: {err}")
        except requests.exceptions.JSONDecodeError as err:
            logging.error(f"Erro ao decodificar JSON: {err}")
        except requests.exceptions.RequestException as err:
            logging.error(f"Erro na requisição: {err}")
        except Exception as err:
            logging.error(f"Erro inesperado: {err}")
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
            logging.warning('Retornando lista vazia pois não foi possível extrair os dados')
            return []
            
        self.data = data
        return self.data
    
    def save_to_json(self, file_path=None, pretty=True) -> bool:
        """
        Salva os dados da API em um arquivo JSON.
        
        Args:
            file_path: Caminho onde o arquivo será salvo. Se None, usa o caminho padrão.
            pretty: Se True, formata o JSON com indentação
            
        Returns:
            True se o arquivo foi salvo com sucesso, False caso contrário
        """
        
        file_path = file_path or self.file_path # caminho default
        
        if not self.data: # não tem dados?
            self.data = self.get_data()
            
            if not self.data: # ainda nao tem dados?
                logging.error("Não há dados para salvar")
                return False
                
        # criar o diretório se não existir
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                indent = 4 if pretty else None
                json.dump(self.data, f, indent=indent, ensure_ascii=False)
            logging.info(f"Dados salvos com sucesso em {file_path}")
            return True
        except Exception as err:
            logging.error(f"Erro ao salvar arquivo JSON: {err}")
            return False

# --- testes de execução
if __name__ == '__main__':
    try:
        extractor = APIExtractor()
        data = extractor.get_data()
        if data:
            print(f"\nDados obtidos com sucesso. Total: {len(data)}")
            print(json.dumps(data[:2], indent=4, ensure_ascii=False))
            extractor.save_to_json()
        else:
            print("\nNão foi possível obter dados da API")
    except ValueError as e:
        print(f"\nErro: {e}")