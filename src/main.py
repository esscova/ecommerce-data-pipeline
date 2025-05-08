"""
Script principal para executar o pipeline ETL.
Este script coordena o fluxo completo do ETL, orquestrando a extração de dados da API,
a transformação dos dados conforme as regras de negócio, e o carregamento dos dados
processados no banco de dados MongoDB.
"""

from pipeline.etl import ETL

def main():
    try:
        pipeline = ETL()
        sucesso = pipeline.executar_pipeline()
        
        if sucesso:
            print("Pipeline concluído com sucesso!\n")
        else:
            print("Pipeline encontrou problemas durante a execução.")
            
    except Exception as e:
        print(f"Erro: {e}")

if __name__ == "__main__":
    main()