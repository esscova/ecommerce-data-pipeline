"""
Módulo principal do pipeline ETL (Extract, Transform, Load) para processamento de dados.
Este módulo coordena o fluxo completo do ETL, orquestrando a extração de dados da API,
a transformação dos dados conforme as regras de negócio, e o carregamento dos dados
processados no banco de dados MongoDB.
"""

# --- bibliotecas
from extract import APIExtractor
from transform import Transform
from load_mongo import MongoDBLoader
import logging
from typing import List, Dict, Any
from dotenv import load_dotenv

# --- configs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('etl_pipeline')

# variáveis de ambiente [src/.env]
load_dotenv()

class ETL:
    """
    Pipeline ETL (Extract, Transform, Load) para processar dados de vendas.
    
    Esta classe orquestra o processo completo de:
    1. Extrair dados da API
    2. Transformar os dados conforme necessário
    3. Carregar os dados em um banco MongoDB
    """
    
    def __init__(self, raw_data_path='src/data/raw/vendas.json', 
                transformed_data_path='src/data/transformed/vendas_transformadas.json'):
        """
        Inicializa o pipeline ETL.
        
        Args:
            raw_data_path: Caminho para salvar os dados brutos
            transformed_data_path: Caminho para salvar os dados transformados
        """
        self.extractor = APIExtractor(file_path=raw_data_path)
        self.transformador = Transform(file_path=transformed_data_path)
        self.loader = MongoDBLoader()
        self.raw_data_path = raw_data_path
        self.transformed_data_path = transformed_data_path
        
    def extrair(self, force_refresh=False) -> List[Dict[str, Any]]:
        """
        Extrai dados da API e opcionalmente os persiste.
        
        Args:
            force_refresh: Se True, força uma nova extração mesmo que já existam dados em cache
            
        Returns:
            Dados extraídos da API
        """
        logger.info("Iniciando fase de EXTRAÇÃO")
        dados = self.extractor.get_data(force_refresh=force_refresh)
        
        if dados:
            logger.info(f"Extração concluída com sucesso. {len(dados)} registros obtidos.")
            # Salva os dados brutos
            self.extractor.save_to_json()
        else:
            logger.error("Falha na extração dos dados. Pipeline interrompido.")
            
        return dados
        
    def transformar(self, dados: List[Dict[str, Any]], save_file=True) -> List[Dict[str, Any]]:
        """
        Transforma os dados extraídos.
        
        Args:
            dados: Dados a serem transformados
            save_file: Se True, salva os dados transformados em arquivo
            
        Returns:
            Dados transformados
        """
        if not dados:
            logger.warning("Nenhum dado para transformar. Pulando fase de TRANSFORMAÇÃO.")
            return []
            
        logger.info("Iniciando fase de TRANSFORMAÇÃO")
        dados_transformados = self.transformador.run(dados, save_file=save_file)
        
        logger.info(f"Transformação concluída. {len(dados_transformados)} registros transformados.")
        return dados_transformados
        
    def carregar(self, dados: List[Dict[str, Any]]) -> bool:
        """
        Carrega os dados transformados no MongoDB.
        
        Args:
            dados: Dados transformados a serem carregados
            
        Returns:
            True se o carregamento foi bem sucedido, False caso contrário
        """
        if not dados:
            logger.warning("Nenhum dado para carregar. Pulando fase de CARGA.")
            return False
            
        logger.info("Iniciando fase de CARGA para MongoDB")
        
        resultado = self.loader.carregar_dados(dados) # mongodbloader para carregar
        
        if resultado:
            logger.info("Carga no MongoDB concluída com sucesso.")
        else:
            logger.error("Falha durante a carga no MongoDB.")
            
        return resultado
                
    def executar_pipeline(self, force_extract=False) -> bool:
        """
        Executa o pipeline ETL completo.
        
        Args:
            force_extract: Se True, força uma nova extração mesmo que já existam dados em cache
            
        Returns:
            True se o pipeline foi executado com sucesso, False caso contrário
        """
        logger.info("\nIniciando pipeline ETL completo")
        
        # 1. extração
        dados_brutos = self.extrair(force_refresh=force_extract)
        if not dados_brutos:
            logger.error("❌ Pipeline interrompido na fase de extração")
            return False
            
        # 2. transformação
        dados_transformados = self.transformar(dados_brutos)
        if not dados_transformados:
            logger.error("❌ Pipeline interrompido na fase de transformação")
            return False
            
        # 3. carga
        resultado_carga = self.carregar(dados_transformados)
        if not resultado_carga:
            logger.error("❌ Pipeline interrompido na fase de carga")
            return False
            
        logger.info("✅ Pipeline ETL executado com sucesso!")
        return True

# --- executar o pipeline
if __name__ == "__main__":
    try:
        pipeline = ETL()
        sucesso = pipeline.executar_pipeline()
        
        if sucesso:
            print("Pipeline concluído com sucesso!\n")
        else:
            print("Pipeline encontrou problemas durante a execução.")
            
    except Exception as e:
        logger.error(f"Erro não tratado durante a execução do pipeline: {e}", exc_info=True)
        print(f"Erro: {e}")
