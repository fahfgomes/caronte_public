import numpy as np
import psycopg2.extras
from serverConnections import database
from datetime import datetime
import pandas as pd
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import os
import duckdb
import gzip


def convert_csv_to_parquet(file_names):
    for file_name in file_names:
        print(f'Convertendo o arquivo {file_name} para parquet.')
        diretorio_destino = f"//caronte//files//{file_name}"
        csv_file = f"{diretorio_destino}//{file_name}.csv"
        parquet_file = f"{diretorio_destino}//{file_name}.parquet"
        
        try:
            con = duckdb.connect(database= ':memory:')
            
            con.execute("SET memory_limit='10GB'")
            
            con.execute(f'''
                        COPY (SELECT * FROM read_csv_auto('{csv_file}', all_varchar=1, header=true)) TO '{parquet_file}'
                        (FORMAT PARQUET, CODEC 'SNAPPY')
                        ''')            
            # Fechar a conexão do DuckDB para garantir que o arquivo Parquet seja finalizado
            con.close()
            
            print(f'Arquivo {parquet_file} gerado.')
            
        except KeyboardInterrupt:
            print("Processo interrompido pelo usuário.")
            return None
        except Exception as e:
            print(f"Erro ao converter {csv_file} para parquet: {str(e)}")
            return None
        
    return parquet_file


def convert_dataframe_to_parquet(df, query_name):
    print(f'Convertendo o dataframe {query_name} para parquet.')
    # Converter o DataFrame para uma tabela PyArrow
    #df = df.astype('str')
    table = pa.Table.from_pandas(df)
    # Definir o caminho e o nome do arquivo Parquet
    parquet_file = f'{query_name}.parquet'
    # Salvar a tabela como arquivo Parquet
    pq.write_table(table, parquet_file)
    print(f'Arquivo {parquet_file} gerado.')
    return parquet_file


def to_s3(dir_name, file_names, pq_name=None, compress=False):
    bucket_name = ''
    key_id = ''
    secret_access_key = ''
    # Configurar credenciais
    session = boto3.Session(
        aws_access_key_id=key_id,
        aws_secret_access_key=secret_access_key
    )
    for file_name in file_names:
        # Verificar se a entrada é um arquivo CSV ou um dataframe
        if isinstance(file_name, str):
            # Converter o arquivo CSV para Parquet
            parquet_file = convert_csv_to_parquet([file_name])
        elif isinstance(file_name, pd.DataFrame):
            # Converter o dataframe para Parquet
            parquet_file = convert_dataframe_to_parquet(file_name, pq_name)
        else:
            print('Entrada inválida. Deve ser um arquivo CSV ou um dataframe.')
            return

        if parquet_file is None:
            return

        # Verificar se a compactação está habilitada
        if compress:
            # Compactar o arquivo Parquet em formato gz
            compressed_file = parquet_file + '.gz'
            with open(parquet_file, 'rb') as f_in:
                with gzip.open(compressed_file, 'wb') as f_out:
                    f_out.writelines(f_in)
            # Atualizar o nome do arquivo para o nome compactado
            os.remove(parquet_file)
            parquet_file = compressed_file

        # Enviar arquivo Parquet para o S3
        s3 = session.client('s3')
        s3_bucket = session.resource('s3').Bucket(bucket_name)
        # Verificar se a pasta existe no bucket
        if pq_name:
            prefix = f'{dir_name}/{pq_name}/'
        else:
            prefix = f'{dir_name}/{file_name}/'
        objects = list(s3_bucket.objects.filter(Prefix=prefix))
        if not objects:
            # A pasta não existe, criar a pasta
            s3_bucket.put_object(Key=prefix)
        # Upload do arquivo Parquet para a pasta no S3
        print(f'Fazendo Upload do arquivo {parquet_file} para o S3.')
        
        # Verifica se o arquivo Parquet está completamente escrito e fechado
        with open(parquet_file, 'rb') as f:
            # Envia o arquivo para o S3
            s3.upload_fileobj(f, bucket_name, prefix + os.path.basename(parquet_file))
        
        # Excluir o arquivo Parquet localmente
        os.remove(parquet_file)
