# 
# Classe que extrai os dados da API e os salva em um arquivo JSON
#

# --- bibliotecas
import requests
import os
from dotenv import load_dotenv
from typing import List, Dict, Any
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
    def __init__(self, url=None, timeout=10):
        self.url = url or API_BASE_URL
        self.timeout = timeout
        if self.url is None:
            raise ValueError('URL da API não informada')

    def __extract(self) -> List[Dict[str, Any]]:
        """
            Extrai dados da API definida na inicialização da classe.
        
            Returns:
                Lista de dicionários com os dados da API ou None em caso de erro.
        """
        logging.info('Iniciando extração dos dados da API')
        
        try:
            response = requests.get(self.url, timeout=self.timeout)
            response.raise_for_status()
            dados = response.json()
            
            logging.info(f'Coleta de dados bem sucedida: {response.status_code}')
            logging.info(f'Tamanho da coleta: {len(response.json())}')
            
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
    
    def get_data(self) -> List[Dict[str, Any]]:
        """
            Obtém os dados da API com validação.
        
            Returns:
                Lista de dicionários com os dados ou lista vazia em caso de erro.
        """
        data = self.__extract()
        
        if data is None:
            logging.warning('Retornando lista vazia pois nao foi possivel extrair os dados')
            return []
        
        return data
    
    def save_to_json(self, file_path: str='src/data/dados.json', pretty=True) -> bool:
            """
            Salva os dados da API em um arquivo JSON.
            
            Args:
                file_path: Caminho onde o arquivo será salvo
                pretty: Se True, formata o JSON com indentação
                
            Returns:
                True se o arquivo foi salvo com sucesso, False caso contrário
            """
            data = self.get_data()
            if not data:
                return False
                
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    indent = 4 if pretty else None
                    json.dump(data, f, indent=indent, ensure_ascii=False)
                logging.info(f"Dados salvos com sucesso em {file_path}")
                return True
            except Exception as err:
                logging.error(f"Erro ao salvar arquivo JSON: {err}")
                return False
 
# --- testes de execução
if __name__ == '__main__':
    import json
    extractor = APIExtractor()
    data = extractor.get_data()
    if data:extractor.save_to_json()

