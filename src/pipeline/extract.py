# 
# Classe que extrai os dados da API
#

# --- bibliotecas
import requests
import os
from dotenv import load_dotenv
from typing import List, Dict, Any
import logging

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
        logging.info('Iniciando extração dos dados da API')
        try:
            response = requests.get(self.url)
            response.raise_for_status()
            logging.info(f'Coleta de dados bem sucedida: {response.status_code}')
            logging.info(f'Tamanho da coleta: {len(response.json())}')
            return response.json()
        except requests.exceptions.HTTPError as err:
            logging.error("Erro request: ", err)
        except Exception as err:
            logging.error("Erro geral: ", err)
        return []
    
    def get_data(self) -> List[Dict[str, Any]]:
        return self.__extract()
 
# --- testes de execução
if __name__ == '__main__':
    import json
    extractor = APIExtractor()
    data = extractor.get_data()
    print(json.dumps(data[:3], indent=4, ensure_ascii=False))

