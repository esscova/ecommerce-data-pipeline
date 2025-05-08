"""
Este script é responsável por executar o pipeline ETL.
"""

from pipeline.etl import ETL

if __name__ == "__main__":
    try:
        pipeline = ETL()
        sucesso = pipeline.executar_pipeline()
        
        if sucesso:
            print("Pipeline concluído com sucesso!\n")
        else:
            print("Pipeline encontrou problemas durante a execução.")
            
    except Exception as e:
        print(f"Erro: {e}")