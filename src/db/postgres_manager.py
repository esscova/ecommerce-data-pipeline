"""
Módulo para gerenciamento de conexões e operações com o PostgreSQL.
"""

# --- Bibliotecas ---
import psycopg2
import psycopg2.extras 
import os
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional, Tuple, Union
import logging
import glob

load_dotenv()

# --- Configuração de logging 
logging.basicConfig(level=os.getenv('LOG_LEVEL', 'INFO').upper(),
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Classe
class PostgresManager:
    def __init__(self,
                 db_host: Optional[str] = None,
                 db_port: Optional[str] = None,
                 db_name: Optional[str] = None,
                 db_user: Optional[str] = None,
                 db_password: Optional[str] = None):

        self.db_host = db_host or os.getenv('POSTGRES_HOST')
        self.db_port = db_port or os.getenv('POSTGRES_PORT', '5432')
        self.db_name = db_name or os.getenv('POSTGRES_DB')
        self.db_user = db_user or os.getenv('POSTGRES_USER')
        self.db_password = db_password or os.getenv('POSTGRES_PASSWORD')

        self.connection: Optional[psycopg2.extensions.connection] = None
        self.cursor: Optional[psycopg2.extensions.cursor] = None
        self.is_connected: bool = False

        if not all([self.db_host, self.db_name, self.db_user, self.db_password]):
            msg = "Variáveis de ambiente do PostgreSQL não configuradas corretamente (POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD)."
            logger.error(msg)
            raise ValueError(msg)

    def connect(self) -> bool:
        if self.is_connected and self.connection:
            logger.debug(f"Já conectado ao PostgreSQL: {self.db_name}@{self.db_host}")
            return True
        try:
            logger.debug(f"Tentando conectar ao PostgreSQL: {self.db_name}@{self.db_host}")
            self.connection = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                dbname=self.db_name,
                user=self.db_user,
                password=self.db_password
            )
            self.cursor = self.connection.cursor()
            self.is_connected = True
            logger.info(f"Conexão com PostgreSQL estabelecida: {self.db_name}@{self.db_host}")
            return True
        except psycopg2.Error as e:
            logger.error(f"Erro ao conectar com PostgreSQL ({self.db_name}@{self.db_host}): {e}")
            self.connection = None
            self.cursor = None
            self.is_connected = False
            return False

    def disconnect(self) -> None:
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.connection:
            self.connection.close()
            self.connection = None
        self.is_connected = False
        logger.info(f"Conexão com PostgreSQL fechada: {self.db_name}@{self.db_host}")

    def __enter__(self):
        if self.connect():
            return self
        raise ConnectionError(f"Falha ao conectar ao PostgreSQL ({self.db_name}@{self.db_host}) no __enter__.")

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.connection: # conexão nem foi estabelecida
            self.disconnect() # forçar que tudo seja resetado
            return False

        try:
            if exc_type: # exceção foi levantada?
                self.connection.rollback()
                logger.warning(f"Rollback da transação PostgreSQL devido à exceção: {exc_val}")
            else: 
                self.connection.commit()
                logger.debug("Commit da transação PostgreSQL bem-sucedido.")
        except psycopg2.Error as db_err:
            logger.error(f"Erro durante o commit/rollback da transação PostgreSQL: {db_err}")
            
            try: # rollback final em caso de erro no commit
                if self.connection and not self.connection.closed: # Verifica se a conexão ainda está aberta
                    self.connection.rollback()
                    logger.warning("Rollback da transação PostgreSQL após erro no commit/rollback.")
            except psycopg2.Error as rb_final_err:
                logger.error(f"Erro durante o rollback final da transação: {rb_final_err}")
        finally:
            self.disconnect()
        return False

    def execute_query(self, query: str, params: Optional[Union[Tuple, List[Tuple]]] = None, many: bool = False) -> None:
        """
        Executa uma query SQL (DDL ou DML que não retorna resultados significativos como SELECT).
        Para múltiplas inserções/updates, use execute_batch ou execute_values.
        """
        if not self.is_connected or not self.cursor:
            msg = "Não conectado ao PostgreSQL. Não é possível executar a query."
            logger.error(msg)
            raise ConnectionError(msg) # levanta erro se não conectado
        try:
            if many and params: # executemany
                self.cursor.executemany(query, params)
                logger.debug(f"Query (executemany) executada: {query[:100]}... com {len(params)} conjuntos de params.")
            else:
                self.cursor.execute(query, params)
                logger.debug(f"Query executada: {query[:100]}... com params: {params}")
        except psycopg2.Error as e:
            logger.error(f"Erro ao executar query no PostgreSQL: {e}")
            logger.error(f"Query problemática: {query} com params: {params}")
            raise 

    def fetch_all(self, query: str, params: Optional[Tuple] = None) -> List[Tuple]:
        """Executa uma query SELECT e retorna todos os resultados."""
        if not self.is_connected or not self.cursor:
            msg = "Não conectado ao PostgreSQL. Não é possível buscar dados."
            logger.error(msg)
            raise ConnectionError(msg)
        try:
            self.cursor.execute(query, params)
            logger.debug(f"Query de busca executada: {query[:100]}... com params: {params}")
            return self.cursor.fetchall()
        except psycopg2.Error as e:
            logger.error(f"Erro ao buscar dados no PostgreSQL: {e}")
            logger.error(f"Query problemática: {query} com params: {params}")
            raise

    def fetch_one(self, query: str, params: Optional[Tuple] = None) -> Optional[Tuple]:
        """Executa uma query SELECT e retorna um único resultado."""
        if not self.is_connected or not self.cursor:
            msg = "Não conectado ao PostgreSQL. Não é possível buscar dados."
            logger.error(msg)
            raise ConnectionError(msg)
        try:
            self.cursor.execute(query, params)
            logger.debug(f"Query de busca (one) executada: {query[:100]}... com params: {params}")
            return self.cursor.fetchone()
        except psycopg2.Error as e:
            logger.error(f"Erro ao buscar um dado no PostgreSQL: {e}")
            logger.error(f"Query problemática: {query} com params: {params}")
            raise

    def create_table_if_not_exists(self, table_name: str, columns_definition_sql: str):
        """
        Cria uma tabela no PostgreSQL se ela não existir.

        Args:
            table_name: Nome da tabela a ser criada.
            columns_definition_sql: String SQL com a definição das colunas.
                                    Ex: "product_id VARCHAR(255) PRIMARY KEY, product_name TEXT"
        """
        create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_definition_sql});"
        self.execute_query(create_query)
        logger.info(f"Tabela '{table_name}' verificada/criada com sucesso.")

    def truncate_table(self, table_name: str):
        """Limpa (TRUNCATE) todos os dados de uma tabela."""
        truncate_query = f"TRUNCATE TABLE {table_name};"
        self.execute_query(truncate_query)
        logger.warning(f"Todos os dados da tabela '{table_name}' foram removidos (TRUNCATE).")

    def load_data_to_staging(self,
                             table_name: str,
                             data: List[Dict[str, Any]],
                             column_order: List[str]):
        """
        Carrega dados em uma tabela de staging (geralmente após um TRUNCATE).
        Usa psycopg2.extras.execute_values para inserção em lote eficiente.

        Args:
            table_name: Nome da tabela de staging.
            data: Lista de dicionários contendo os dados a serem inseridos.
            column_order: Lista de strings com os nomes das colunas na ordem correta
                          para inserção, correspondendo às chaves dos dicionários em 'data'.
        """
        if not self.is_connected or not self.cursor:
            msg = "Não conectado ao PostgreSQL. Não é possível carregar os dados."
            logger.error(msg)
            raise ConnectionError(msg)
        if not data:
            logger.info(f"Nenhum dado para carregar na tabela de staging '{table_name}'.")
            return True # considerar sucesso, pois não há o que fazer

        # preparar os valores como uma lista de tuplas na ordem especificada por column_order
        # garantir que a ordem dos valores em cada tupla corresponda a column_order
        try:
            values_to_insert = []
            for item in data: # tupla na ordem correta, usando None para chaves ausentes, banco deve permitir nulos ou ter default
                row_values = tuple(item.get(col) for col in column_order)
                values_to_insert.append(row_values)
        except Exception as e_prep:
            logger.error(f"Erro ao preparar dados para inserção na tabela '{table_name}': {e_prep}", exc_info=True)
            raise ValueError(f"Erro na preparação dos dados para a tabela '{table_name}'. Verifique as chaves e column_order.") from e_prep


        cols_sql = ", ".join(f'"{col}"' for col in column_order) 
        insert_sql = f"INSERT INTO \"{table_name}\" ({cols_sql}) VALUES %s" 

        try:
            psycopg2.extras.execute_values(self.cursor, insert_sql, values_to_insert, page_size=1000) # page_size para grandes volumes
            logger.info(f"{len(values_to_insert)} registros inseridos com sucesso na tabela de staging '{table_name}'.")
            return True
        except psycopg2.Error as e:
            logger.error(f"Erro ao carregar dados na tabela de staging '{table_name}' do PostgreSQL: {e}")
            logger.error(f"Query de carga (início): {insert_sql[:200]}...")
            raise # relevanta para que o __exit__ possa fazer rollback


    def execute_sql_file(self, file_path: str):
        """Executa um arquivo SQL."""
        if not self.is_connected or not self.cursor:
            msg = "Não conectado ao PostgreSQL. Não é possível executar o arquivo SQL."
            logger.error(msg)
            raise ConnectionError(msg)
        try:
            with open(file_path, 'r') as f:
                sql_script = f.read()
            self.cursor.execute(sql_script) 
            logger.info(f"Arquivo SQL '{file_path}' executado com sucesso.")
        except psycopg2.Error as e:
            logger.error(f"Erro ao executar arquivo SQL '{file_path}': {e}")
            raise
        except FileNotFoundError:
            logger.error(f"Arquivo SQL não encontrado: {file_path}")
            raise

    def setup_database_schema(self, schema_scripts_dir: str):
        """
        Executa todos os arquivos .sql em um diretório para configurar o esquema.
        Os arquivos são executados em ordem alfanumérica.
        """
        if not self.is_connected:
            logger.error("Não conectado ao PostgreSQL. Não é possível configurar o esquema.")
            return False 

        logger.info(f"Configurando esquema do banco de dados a partir de scripts em: {schema_scripts_dir}")
        sql_files = sorted(glob.glob(os.path.join(schema_scripts_dir, "*.sql"))) # Garante a ordem

        if not sql_files:
            logger.warning(f"Nenhum arquivo .sql encontrado em '{schema_scripts_dir}'. Esquema não alterado.")
            return True # considerar sucesso, pois apenas nada a fazer

        for sql_file in sql_files:
            try:
                logger.info(f"Executando script de esquema: {sql_file}")
                self.execute_sql_file(sql_file)
            except Exception as e:
                logger.error(f"Falha ao executar script de esquema '{sql_file}': {e}. Parando configuração do esquema.", exc_info=True)
                return False # falha na configuração do esquema
        logger.info("Configuração do esquema do banco de dados concluída com sucesso.")
        return True
# --- 