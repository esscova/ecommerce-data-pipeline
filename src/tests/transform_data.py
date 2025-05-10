"""
Módulo de transformação de dados para o pipeline ETL.
Este módulo é responsável por todas as transformações aplicadas aos dados brutos,
incluindo renomeação de campos e normalização de valores.
As transformações são focadas em preparar os dados para análise e armazenamento,
sem envolver qualquer persistência de dados, seguindo o princípio de responsabilidade única.
"""

# --- bibliotecas
import logging
from typing import List, Dict, Any
from datetime import datetime

# configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- classe
class Transform:
    def __init__(self):
        """
        Inicializa a classe de transformação de dados.
        """
        self.data = []
        
    def _renomear_chaves(self, lista_dicts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Função para renomear as chaves 'lat' e 'lon' para 'latitude' e 'longitude'
        em uma lista de dicionários.
        
        Args:
            lista_dicts: Lista contendo dicionários com as chaves 'lat' e 'lon'
            
        Returns:
            Lista com os dicionários atualizados
        """
        logging.info("Renomeando chaves geográficas (lat/lon → latitude/longitude)")
        
        for dicionario in lista_dicts:
            if 'lat' in dicionario:
                dicionario['latitude'] = dicionario.pop('lat')
            if 'lon' in dicionario:
                dicionario['longitude'] = dicionario.pop('lon')
                
        return lista_dicts
        
    def _normalizar_categorias(self, lista_dicts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Normaliza as categorias de produtos (converte para minúsculas e remove espaços extras).
        
        Args:
            lista_dicts: Lista contendo dicionários com a chave 'Categoria do Produto'
            
        Returns:
            Lista com os dicionários atualizados
        """
        logging.info("Normalizando categorias de produtos")
        
        for item in lista_dicts:
            if 'Categoria do Produto' in item:
                item['Categoria do Produto'] = item['Categoria do Produto'].lower().strip()
                
        return lista_dicts
           
    def _converter_datas(self, lista_dicts: List[Dict[str, Any]], campos_data: List[str], formato: str = '%Y-%m-%d') -> List[Dict[str, Any]]:
        """
        Converte os campos de texto contendo datas para objetos date do Python.
        
        Args:
            lista_dicts: Lista contendo dicionários com campos de data
            campos_data: Lista com os nomes dos campos que contêm datas
            formato: Formato da data nos dados de entrada (padrão: '%Y-%m-%d')
            
        Returns:
            Lista com os dicionários atualizados
        """
        logging.info(f"Convertendo campos de data: {', '.join(campos_data)}")
        
        for item in lista_dicts:
            for campo in campos_data:
                if campo in item and item[campo]:
                    try:
                        # string -> datetime -> data
                        item[campo] = datetime.strptime(item[campo], formato).date()
                    except ValueError:
                        logging.warning(f"Não foi possível converter data '{item[campo]}' no formato {formato} para o item: {item.get('Produto', 'desconhecido')}")
                        
        return lista_dicts
    
    
    def transform_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Executa todas as transformações nos dados.
        
        Args:
            data: Lista de dicionários a serem transformados
            
        Returns:
            Lista com os dados transformados
        """
        if not data:
            logging.warning("Nenhum dado para transformar")
            return []
            
        logging.info(f"Iniciando transformação de {len(data)} registros")
        
        # executando transformações
        transformed_data = data.copy()
        transformed_data = self._renomear_chaves(transformed_data)
        transformed_data = self._normalizar_categorias(transformed_data)
        transformed_data = self._converter_datas(transformed_data, ['Data da Compra'], '%d/%m/%Y')
        
        self.data = transformed_data
        
        logging.info(f"Transformação concluída. {len(self.data)} registros transformados")
        return self.data