# 
# script para execturar o pipeline
#

# --- bibliotecas
from pipeline.extract import APIExtractor

# --- funcoes
def extract() -> None:
    extractor = APIExtractor()
    data = extractor.get_data()
    if data:extractor.save_to_json()

# --- funcao principal
if __name__ == '__main__':
    extract()