"""
Módulo para operações de carga (load) de dados em diferentes destinos.
Este módulo é responsável por todas as operações relacionadas ao carregamento
de dados em diferentes destinos, como MongoDB (dados brutos) e PostgreSQL (data mart).
"""

# --- bibliotecas
import logging
import os
import pymongo
import psycopg2
import psycopg2.extras
from typing import List, Dict, Any
from dotenv import load_dotenv
from datetime import datetime, date

# --- variaveis de ambiente
load_dotenv()

# MongoDB
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB = os.getenv('MONGO_DB', 'analytics')
MONGO_RAW_COLLECTION = os.getenv('MONGO_RAW_COLLECTION', 'raw_data')

# PostgreSQL
PG_HOST = os.getenv('PG_HOST', 'localhost') 
PG_PORT = os.getenv('PG_PORT', '5432')
PG_DB = os.getenv('PG_DB', 'data_mart')
PG_USER = os.getenv('PG_USER', 'postgres')
PG_PASSWORD = os.getenv('PG_PASSWORD', '')
PG_SCHEMA = os.getenv('PG_SCHEMA', 'public')
PG_TABLE = os.getenv('PG_TABLE', 'vendas')

# --- configs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('data_loader')

# --- classes
class MongoDBLoader:
    """
    Classe responsável pelo carregamento de dados brutos no MongoDB.
    """
    
    def __init__(self, mongo_uri=None, db_name=None, collection_name=None):
        """
        Inicializa o carregador de dados para MongoDB.
        
        Args:
            mongo_uri: URI de conexão com o MongoDB (opcional, usa MONGO_URI do .env por padrão)
            db_name: Nome do banco de dados MongoDB (opcional, usa MONGO_DB do .env por padrão)
            collection_name: Nome da coleção MongoDB (opcional, usa MONGO_RAW_COLLECTION do .env por padrão)
        """
        self.mongo_uri = mongo_uri or MONGO_URI
        self.mongo_db = db_name or MONGO_DB
        self.mongo_collection = collection_name or MONGO_RAW_COLLECTION
        self.mongo_client = None
        
        if not self.mongo_uri:
            logger.error("URI de conexão do MongoDB não configurada")
            raise ValueError("URI de conexão do MongoDB não configurada")
            
        logger.info(f"MongoDB Loader configurado para DB: {self.mongo_db}, Coleção: {self.mongo_collection}")
        
    def _conectar_mongodb(self) -> bool:
        """
        Estabelece uma conexão com o banco de dados MongoDB.
        
        Returns:
            True se a conexão foi bem sucedida, False caso contrário
        """
        try:
            self.mongo_client = pymongo.MongoClient(self.mongo_uri)
            self.mongo_client.admin.command('ping')
            logger.info("Conexão com MongoDB estabelecida com sucesso")
            return True
        except pymongo.errors.ConnectionFailure as e:
            logger.error(f"Falha ao conectar com MongoDB: {e}")
            return False
        except Exception as e:
            logger.error(f"Erro ao conectar com MongoDB: {e}")
            return False
    
    def _desconectar_mongodb(self):
        """
        Fecha a conexão com o MongoDB se estiver aberta.
        """
        if self.mongo_client:
            self.mongo_client.close()
            logger.info("Conexão com MongoDB fechada")
    
    def _adicionar_metadados_ingestao(self, dados: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Adiciona metadados de ingestão aos documentos.
        
        Args:
            dados: Lista de documentos a receber metadados
            
        Returns:
            Lista de documentos com metadados adicionados
        """
        timestamp = datetime.now()
        
        for documento in dados:
            documento['_metadata'] = {
                'ingestao_timestamp': timestamp,
                'origem': 'api_externa',
                'versao_pipeline': '2.0'
            }
        
        return dados
    
    def carregar_dados_brutos(self, dados: List[Dict[str, Any]]) -> bool:
        """
        Carrega os dados brutos no MongoDB.
        
        Args:
            dados: Lista de documentos (dicionários) a serem inseridos no MongoDB
            
        Returns:
            True se o carregamento foi bem sucedido, False caso contrário
        """
        if not dados:
            logger.warning("Nenhum dado para carregar no MongoDB")
            return False
            
        # Adiciona metadados de ingestão
        dados_com_metadados = self._adicionar_metadados_ingestao(dados)
            
        # Conectar no MongoDB
        if not self._conectar_mongodb():
            return False
            
        try:
            db = self.mongo_client[self.mongo_db]
            collection = db[self.mongo_collection]
            result = collection.insert_many(dados_com_metadados)
            logger.info(f"Carga concluída. {len(result.inserted_ids)} documentos inseridos no MongoDB.")
            return True
            
        except pymongo.errors.BulkWriteError as e:
            logger.error(f"Erro durante inserção em massa no MongoDB: {e}")
            return False
        except Exception as e:
            logger.error(f"Erro ao carregar dados brutos no MongoDB: {e}")
            return False
        finally:
            self._desconectar_mongodb()


class PostgreSQLLoader:
    """
    Classe responsável pelo carregamento de dados transformados no PostgreSQL (data mart).
    """
    
    def __init__(self, host=None, port=None, dbname=None, user=None, password=None, 
                 schema=None, table=None):
        """
        Inicializa o carregador de dados para PostgreSQL.
        
        Args:
            host: Host do PostgreSQL
            port: Porta do PostgreSQL
            dbname: Nome do banco de dados
            user: Usuário
            password: Senha
            schema: Schema onde a tabela está localizada
            table: Nome da tabela de destino
        """
        self.host = host or PG_HOST
        self.port = port or PG_PORT
        self.dbname = dbname or PG_DB
        self.user = user or PG_USER
        self.password = password or PG_PASSWORD
        self.schema = schema or PG_SCHEMA
        self.table = table or PG_TABLE
        self.conn = None
        
        logger.info(f"PostgreSQL Loader configurado para {self.dbname}.{self.schema}.{self.table}")
    
    def _conectar_postgres(self) -> bool:
        """
        Estabelece uma conexão com o banco de dados PostgreSQL.
        
        Returns:
            True se a conexão foi bem sucedida, False caso contrário
        """
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )
            logger.info("Conexão com PostgreSQL estabelecida com sucesso")
            return True
        except psycopg2.Error as e:
            logger.error(f"Erro ao conectar ao PostgreSQL: {e}")
            return False
    
    def _desconectar_postgres(self):
        """
        Fecha a conexão com o PostgreSQL se estiver aberta.
        """
        if self.conn:
            self.conn.close()
            logger.info("Conexão com PostgreSQL fechada")
    
    def _criar_tabela_se_nao_existir(self) -> bool:
        """
        Cria a tabela de destino se ela não existir.
        
        Returns:
            True se a tabela já existia ou foi criada com sucesso, False caso contrário
        """
        if not self.conn:
            logger.error("Tentativa de criar tabela sem conexão estabelecida")
            return False
            
        try:
            with self.conn.cursor() as cursor:
                # Verifica se o schema existe, se não, cria
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
                
                # Cria a tabela se não existir
                cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.{self.table} (
                    id SERIAL PRIMARY KEY,
                    produto VARCHAR(255),
                    "Categoria do Produto" VARCHAR(100),
                    "Preço" NUMERIC(10, 2),
                    "Frete" NUMERIC(10, 2),
                    "Valor Total" NUMERIC(10, 2),
                    "Data da Compra" DATE,
                    latitude NUMERIC(10, 6),
                    longitude NUMERIC(10, 6),
                    data_processamento TIMESTAMP,
                    versao_transformacao VARCHAR(10),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """)
                self.conn.commit()
                logger.info(f"Tabela {self.schema}.{self.table} verificada/criada com sucesso")
                return True
        except psycopg2.Error as e:
            logger.error(f"Erro ao criar tabela: {e}")
            self.conn.rollback()
            return False
    
    def _converter_python_para_sql(self, valor): # ir para transform_data
        """
        Converte tipos de dados Python para formatos compatíveis com SQL.
        
        Args:
            valor: Valor a ser convertido
            
        Returns:
            Valor convertido para formato compatível com SQL
        """
        if isinstance(valor, date) and not isinstance(valor, datetime):
            return valor.isoformat()
        return valor
    
    def carregar_dados_transformados(self, dados: List[Dict[str, Any]]) -> bool:
        """
        Carrega os dados transformados no PostgreSQL.
        
        Args:
            dados: Lista de dicionários com os dados transformados
            
        Returns:
            True se o carregamento foi bem sucedido, False caso contrário
        """
        if not dados:
            logger.warning("Nenhum dado para carregar no PostgreSQL")
            return False
            
        # Conectar ao PostgreSQL
        if not self._conectar_postgres():
            return False
            
        try:
            # Garantir que a tabela existe
            if not self._criar_tabela_se_nao_existir():
                return False
                
            # Preparar colunas e valores para inserção
            with self.conn.cursor() as cursor:
                # Pegar as colunas do primeiro dicionário (assumindo estrutura consistente)
                colunas = list(dados[0].keys())
                
                # Criar a consulta SQL de inserção
                columns_str = ", ".join([f'"{col}"' for col in colunas])
                placeholders = ", ".join(["%s"] * len(colunas))
                
                insert_query = f"""
                INSERT INTO {self.schema}.{self.table} ({columns_str})
                VALUES ({placeholders})
                """
                
                # Preparar os valores para inserção
                valores_para_inserir = []
                for item in dados:
                    valores = [self._converter_python_para_sql(item.get(col)) for col in colunas]
                    valores_para_inserir.append(valores)
                
                # Executar a inserção em massa
                psycopg2.extras.execute_batch(cursor, insert_query, valores_para_inserir)
                self.conn.commit()
                
                logger.info(f"Carga concluída. {len(dados)} registros inseridos no PostgreSQL.")
                return True
                
        except psycopg2.Error as e:
            logger.error(f"Erro ao inserir dados no PostgreSQL: {e}")
            self.conn.rollback()
            return False
        except Exception as e:
            logger.error(f"Erro inesperado ao carregar dados no PostgreSQL: {e}")
            self.conn.rollback()
            return False
        finally:
            self._desconectar_postgres()