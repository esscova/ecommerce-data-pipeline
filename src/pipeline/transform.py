#
# Script responsável por transformar os dados
#

# --- bibliotecas
import os
import json
import logging
from typing import List, Dict, Any

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- Classe
class Transform:
    def __init__(self, file_path='src/data/transformed/dados_transformados.json', pretty=True):
        """
        Inicializa a classe de transformação de dados.
        
        Args:
            file_path: Caminho onde o arquivo transformado será salvo
            pretty: Se True, formata o JSON com indentação
        """
        self.file_path = file_path
        self.pretty = pretty
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
        
    def _adicionar_valor_total(self, lista_dicts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Adiciona um campo 'Valor Total' somando o preço e o frete.
        
        Args:
            lista_dicts: Lista contendo dicionários com as chaves 'Preço' e 'Frete'
            
        Returns:
            Lista com os dicionários atualizados
        """
        logging.info("Adicionando campo de valor total (preço + frete)")
        
        for item in lista_dicts:
            if 'Preço' in item and 'Frete' in item:
                try:
                    item['Valor Total'] = float(item['Preço']) + float(item['Frete'])
                except (ValueError, TypeError):
                    logging.warning(f"Não foi possível calcular valor total para o item: {item.get('Produto', 'desconhecido')}")
                    
        return lista_dicts
    
    def run(self, data: List[Dict[str, Any]], save_file=False) -> List[Dict[str, Any]]:
        """
        Executa todas as transformações nos dados e opcionalmente salva o resultado.
        
        Args:
            data: Lista de dicionários a serem transformados
            save_file: Se True, salva os dados transformados no arquivo configurado
            
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
        transformed_data = self._adicionar_valor_total(transformed_data)
        
        self.data = transformed_data
        
        if save_file:
            self._save_to_file()
            logging.info(f"Dados salvos no arquivo {self.file_path}")
        else:
            logging.info("Dados transformados sem salvamento em arquivo")
            
        logging.info(f"Transformação concluída. {len(self.data)} registros transformados")
        return self.data
        
    def _save_to_file(self) -> bool:
        """
        Salva os dados transformados em um arquivo.
        
        Returns:
            True se o arquivo foi salvo com sucesso, False caso contrário
        """
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
        
        try:
            with open(self.file_path, 'w', encoding='utf-8') as file:
                indent = 4 if self.pretty else None
                json.dump(self.data, file, indent=indent, ensure_ascii=False)
                
            logging.info(f"Dados transformados salvos com sucesso em {self.file_path}")
            return True
            
        except Exception as err:
            logging.error(f"Erro ao salvar arquivo transformado: {err}")
            return False


# --- testes
if __name__ == "__main__":
    try:
        with open('src/data/raw/dados.json', 'r', encoding='utf-8') as f:
            dados = json.load(f)
            
        transformador = Transform()
        dados_transformados = transformador.run(dados)
        
        print(f"\nAmostra dos dados transformados ({len(dados_transformados)} registros):")
        for i, item in enumerate(dados_transformados[:2]):  
            print(f"\nItem {i+1}:")
            for chave, valor in item.items():
                print(f"  {chave}: {valor}")
                
    except FileNotFoundError:
        logging.error("Arquivo de dados não encontrado.")
    except json.JSONDecodeError:
        logging.error("Erro ao decodificar o arquivo JSON.")
    except Exception as e:
        logging.error(f"Erro inesperado: {e}")