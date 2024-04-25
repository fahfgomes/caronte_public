from serverConnections import database

def log_pipeline(pipeline_name, mensagem, falha, inicio_execucao, fim_execucao):
    # Conectar ao banco de dados
    with database('DW-REDSHIFT') as conn:
        # Abrir um cursor para executar comandos SQL
        with conn.cursor() as cur:
            try:
                # Consultar o ID da pipeline com o nome fornecido
                cur.execute("SELECT id FROM caronte.agenda_pipelines WHERE nome_pipeline = %s", (pipeline_name,))
                pipeline_id = cur.fetchone()

                # Se a pipeline não for encontrada, levantar uma exceção
                if not pipeline_id:
                    raise Exception("Pipeline não encontrada.")

                # Extrair o ID da tupla retornada pela consulta
                pipeline_id = pipeline_id[0]

                # Obtém o tempo de execução da pipeline
                calculo_execucao = fim_execucao - inicio_execucao
                tempo_execucao = str(calculo_execucao)[:11]

                # Inserir uma nova linha na tabela com as informações fornecidas e o ID da pipeline
                cur.execute("""
                    INSERT INTO caronte.tbpipelinelogs (pipeline_id, mensagem, falha, tempo_execucao, inicio_execucao, fim_execucao)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (pipeline_id, mensagem, falha, tempo_execucao, inicio_execucao, fim_execucao))

                # Salvar as mudanças no banco de dados
                conn.commit()

            except Exception as e:
                print(f"Erro ao registrar o log da pipeline: {str(e)}")
