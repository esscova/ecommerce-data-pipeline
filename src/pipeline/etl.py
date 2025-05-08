# 
# script para execturar o pipeline
#

# --- bibliotecas
from src.pipeline.extract import APIExtractor
from src.pipeline.transform import Transform
import logging

# --- configs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class ETL:
    def __init__(self):
        self.extractor = APIExtractor()
        self.transformador = Transform()
    
    def coletar_dados(self):
        data = self.extractor.get_data()
        return data
    
    def persistir_dados(self, data):
        self.extractor.save_to_json(data)
    
    def transformar_dados(self, data):
        dados_transformados = self.transformador.run(data)
        return dados_transformados
    
    def persistir_dados_transformados(self, dados_transformados):
        self.transformador.run(dados_transformados, save_file=True)

    def carregar_dados_mongo(self):
        pass