"""
módulo para gerenciamento de conexões e operações com o postgresql.
"""

# --- bibliotecas ---
import os
import psycopg2
import psycopg2.extras
from typing import List, Dict, Any, Optional, Tuple, Union
import logging
import glob

# --- modulo interno ---
from . import config 

# --- configuração de logging do módulo
logger = logging.getLogger(__name__)


# --- classe ---
class PostgresManager:
    def __init__(self,
                 db_host: Optional[str] = None,
                 db_port: Optional[str] = None, # será string do config, psycopg2 aceita
                 db_name: Optional[str] = None,
                 db_user: Optional[str] = None,
                 db_password: Optional[str] = None):

        self.db_host: Optional[str] = db_host or config.POSTGRES_HOST
        self.db_port: Optional[str] = db_port or config.POSTGRES_PORT 
        self.db_name: Optional[str] = db_name or config.POSTGRES_DB_NAME
        self.db_user: Optional[str] = db_user or config.POSTGRES_USER
        self.db_password: Optional[str] = db_password or config.POSTGRES_PASSWORD

        self.connection: Optional[psycopg2.extensions.connection] = None
        self.cursor: Optional[psycopg2.extensions.cursor] = None
        self.is_connected: bool = False

        if not all([self.db_host, self.db_name, self.db_user, self.db_password]):
            msg = "parâmetros de conexão do postgresql não fornecidos nem configurados completamente."
            logger.error(msg)
            raise ValueError(msg) 
        logger.debug(f"PostgresManager instanciado para DB: {self.db_name}@{self.db_host}:{self.db_port}")


    def _reset_connection_state(self):
        """método auxiliar para resetar o estado da conexão."""
        self.cursor = None # fecha o cursor primeiro se ele existir e estiver aberto (no disconnect)
        self.connection = None
        self.is_connected = False

    def connect(self) -> bool:
        if self.is_connected and self.connection and not self.connection.closed:
            logger.debug(f"já conectado ao postgresql: {self.db_name}@{self.db_host}")
            return True
        try:
            logger.debug(f"tentando conectar ao postgresql: {self.db_name}@{self.db_host}:{self.db_port}")
            self.connection = psycopg2.connect(
                host=self.db_host,
                port=self.db_port, # psycopg2.connect aceita string para porta
                dbname=self.db_name,
                user=self.db_user,
                password=self.db_password
                # connect_timeout=5 # timeout para estabelecer a conexão
            )
            self.cursor = self.connection.cursor()
            self.is_connected = True
            logger.info(f"conexão com postgresql estabelecida: {self.db_name}@{self.db_host}")
            return True
        except psycopg2.Error as e: # captura erros específicos do psycopg2 (inclui OperationalError para conexão)
            logger.error(f"erro ao conectar com postgresql ({self.db_name}@{self.db_host}): {e}", exc_info=True)
            self._reset_connection_state()
            return False
        except Exception as e_gen: # captura outros erros inesperados
            logger.error(f"erro inesperado ao tentar conectar com postgresql: {e_gen}", exc_info=True)
            self._reset_connection_state()
            return False

    def disconnect(self) -> None:
        # fecha o cursor se existir e estiver aberto
        if self.cursor and not self.cursor.closed:
            try:
                self.cursor.close()
            except psycopg2.Error as e:
                logger.error(f"erro ao fechar o cursor do postgresql: {e}", exc_info=True)
        # fecha a conexão se existir e estiver aberta
        if self.connection and not self.connection.closed:
            try:
                self.connection.close()
            except psycopg2.Error as e:
                logger.error(f"erro ao fechar a conexão com postgresql: {e}", exc_info=True)
        
        self._reset_connection_state() # reseta os atributos
        logger.info(f"conexão com postgresql ({self.db_name}@{self.db_host}) explicitamente fechada/resetada.")


    def __enter__(self):
        if self.connect():
            return self
        # connect() já loga o erro detalhado
        raise ConnectionError(f"falha ao conectar ao postgresql ({self.db_name}@{self.db_host}) no __enter__.")

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.connection or self.connection.closed:
            # se a conexão não foi estabelecida ou já foi fechada,
            # apenas garante que os atributos de estado sejam resetados.
            self._reset_connection_state()
            logger.debug("bloco 'with' finalizado, conexão já estava fechada ou não foi estabelecida.")
            return False # não suprime exceções

        try:
            if exc_type:
                logger.warning(f"exceção detectada no bloco 'with': {exc_val}. realizando rollback...")
                self.connection.rollback()
                logger.info("rollback da transação postgresql realizado.")
            else:
                logger.debug("nenhuma exceção no bloco 'with'. realizando commit...")
                self.connection.commit()
                logger.info("commit da transação postgresql bem-sucedido.")
        except psycopg2.Error as db_err:
            logger.error(f"erro durante o commit/rollback da transação postgresql: {db_err}", exc_info=True)
            try:
                # tenta um rollback final se o commit falhou ou se o rollback inicial falhou
                if not self.connection.closed: # verifica novamente
                    self.connection.rollback()
                    logger.warning("rollback da transação postgresql realizado após erro no commit/rollback.")
            except psycopg2.Error as rb_final_err:
                logger.error(f"erro durante o rollback final da transação: {rb_final_err}", exc_info=True)
        finally:
            # desconecta e reseta o estado independentemente do que aconteceu acima
            self.disconnect()
        return False # não suprime a exceção original, se houver

    def execute_query(self, query: str, params: Optional[Union[Tuple, List[Tuple]]] = None, many: bool = False) -> None:
        if not self.is_connected or not self.cursor or self.cursor.closed:
            msg = "não conectado ao postgresql ou cursor fechado. não é possível executar a query."
            logger.error(msg)
            raise ConnectionError(msg)
        try:
            if many and params:
                self.cursor.executemany(query, params)
                logger.debug(f"query (executemany) executada: {query[:100]}... com {len(params)} conjuntos de params.")
            else:
                self.cursor.execute(query, params)
                logger.debug(f"query executada: {query[:100]}... com params: {params}")
        except psycopg2.Error as e:
            logger.error(f"erro ao executar query no postgresql: {e}", exc_info=True)
            logger.error(f"query problemática: {self.cursor.query if self.cursor.query else query}") # tenta logar a query renderizada
            raise

    def fetch_all(self, query: str, params: Optional[Tuple] = None) -> List[Tuple]:
        if not self.is_connected or not self.cursor or self.cursor.closed:
            msg = "não conectado ao postgresql ou cursor fechado. não é possível buscar dados."
            logger.error(msg)
            raise ConnectionError(msg)
        try:
            self.cursor.execute(query, params)
            logger.debug(f"query de busca (all) executada: {query[:100]}... com params: {params}")
            return self.cursor.fetchall()
        except psycopg2.Error as e:
            logger.error(f"erro ao buscar todos os dados no postgresql: {e}", exc_info=True)
            logger.error(f"query problemática: {self.cursor.query if self.cursor.query else query}")
            raise

    def fetch_one(self, query: str, params: Optional[Tuple] = None) -> Optional[Tuple]:
        if not self.is_connected or not self.cursor or self.cursor.closed:
            msg = "não conectado ao postgresql ou cursor fechado. não é possível buscar dado."
            logger.error(msg)
            raise ConnectionError(msg)
        try:
            self.cursor.execute(query, params)
            logger.debug(f"query de busca (one) executada: {query[:100]}... com params: {params}")
            return self.cursor.fetchone()
        except psycopg2.Error as e:
            logger.error(f"erro ao buscar um dado no postgresql: {e}", exc_info=True)
            logger.error(f"query problemática: {self.cursor.query if self.cursor.query else query}")
            raise

    def create_table_if_not_exists(self, table_name: str, columns_definition_sql: str):
        # este método executa uma única instrução ddl, o commit será feito pelo __exit__
        logger.info(f"verificando/criando tabela '{table_name}'...")
        create_query = f"CREATE TABLE IF NOT EXISTS \"{table_name}\" ({columns_definition_sql});" # aspas para o nome da tabela
        self.execute_query(create_query)
        logger.info(f"tabela '{table_name}' verificada/criada com sucesso.")

    def truncate_table(self, table_name: str):
        # este método executa uma única instrução ddl, o commit será feito pelo __exit__
        logger.warning(f"removendo todos os dados da tabela '{table_name}' (truncate)...")
        truncate_query = f"TRUNCATE TABLE \"{table_name}\";" # aspas para o nome da tabela
        self.execute_query(truncate_query)
        logger.info(f"todos os dados da tabela '{table_name}' foram removidos.")

    def load_data_to_staging(self,
                             table_name: str,
                             data: List[Dict[str, Any]],
                             column_order: List[str]) -> bool:
        if not self.is_connected or not self.cursor or self.cursor.closed:
            msg = "não conectado ao postgresql ou cursor fechado. não é possível carregar dados."
            logger.error(msg)
            # não levanta exceção aqui, apenas retorna false, pois o chamador pode querer tratar
            return False
        if not data:
            logger.info(f"nenhum dado para carregar na tabela de staging '{table_name}'.")
            return True

        try:
            values_to_insert = [tuple(item.get(col) for col in column_order) for item in data]
        except Exception as e_prep:
            logger.error(f"erro ao preparar dados para inserção na tabela '{table_name}': {e_prep}", exc_info=True)
            # não levanta ValueError aqui, apenas loga e retorna false, o chamador decide
            return False

        # aspas duplas para nomes de colunas e tabelas para segurança (case-sensitive, palavras reservadas)
        cols_sql = ", ".join([f'"{col}"' for col in column_order])
        insert_sql = f"INSERT INTO \"{table_name}\" ({cols_sql}) VALUES %s"

        try:
            psycopg2.extras.execute_values(self.cursor, insert_sql, values_to_insert, page_size=1000)
            logger.info(f"{len(values_to_insert)} registros inseridos com sucesso na tabela de staging '{table_name}'.")
            # o commit será feito pelo __exit__ do bloco 'with'
            return True
        except psycopg2.Error as e:
            logger.error(f"erro ao carregar dados na tabela de staging '{table_name}' do postgresql: {e}", exc_info=True)
            logger.error(f"query de carga (início): {self.cursor.query if self.cursor.query else insert_sql[:200]}...")
            raise # re-levanta para que __exit__ faça rollback
        except Exception as e_gen: # outros erros inesperados
            logger.error(f"erro inesperado ao carregar dados na tabela de staging '{table_name}': {e_gen}", exc_info=True)
            raise


    def execute_sql_file(self, file_path: str):
        if not self.is_connected or not self.cursor or self.cursor.closed:
            msg = "não conectado ao postgresql ou cursor fechado. não é possível executar o arquivo sql."
            logger.error(msg)
            raise ConnectionError(msg)
        try:
            with open(file_path, 'r', encoding='utf-8') as f: # especifica encoding
                sql_script = f.read()
            # para executar múltiplos comandos em um arquivo, eles devem ser separados por ';'
            # e o cursor.execute pode lidar com isso se não houver resultados intermediários esperados.
            # se houver problemas, pode ser necessário dividir o script em comandos individuais.
            self.cursor.execute(sql_script)
            logger.info(f"arquivo sql '{file_path}' executado com sucesso.")
            # o commit será feito pelo __exit__
        except FileNotFoundError:
            logger.error(f"arquivo sql não encontrado: {file_path}")
            raise
        except psycopg2.Error as e:
            logger.error(f"erro ao executar arquivo sql '{file_path}': {e}", exc_info=True)
            logger.error(f"conteúdo do script (início): {sql_script[:500] if 'sql_script' in locals() else 'não lido'}")
            raise
        except Exception as e_gen:
            logger.error(f"erro inesperado ao executar arquivo sql '{file_path}': {e_gen}", exc_info=True)
            raise


    def setup_database_schema(self, schema_scripts_dir: str) -> bool:
        if not self.is_connected: # connect() já terá sido chamado pelo __enter__
            logger.error("não conectado ao postgresql. não é possível configurar o esquema.")
            return False

        logger.info(f"configurando esquema do banco de dados a partir de scripts em: {schema_scripts_dir}")
        # glob.glob não precisa de os.path.abspath se schema_scripts_dir já for absoluto
        sql_files = sorted(glob.glob(os.path.join(schema_scripts_dir, "*.sql")))

        if not sql_files:
            logger.warning(f"nenhum arquivo .sql encontrado em '{schema_scripts_dir}'. esquema não alterado.")
            return True

        for sql_file in sql_files:
            try:
                logger.info(f"executando script de esquema: {sql_file}")
                self.execute_sql_file(sql_file) # execute_sql_file já lida com exceções e as re-levanta
            except Exception as e: # se execute_sql_file re-levantar, capturamos aqui
                logger.error(f"falha ao executar script de esquema '{sql_file}'. parando configuração do esquema.", exc_info=False) # exc_info=False para não duplicar traceback
                # o __exit__ do bloco 'with' que chamou setup_database_schema fará o rollback.
                return False
        logger.info("configuração do esquema do banco de dados concluída com sucesso.")
        return True

# if __name__ == '__main__':
    # ... (bloco de teste) ...