import csv
import io
import openpyxl
import sqlalchemy
import pandas as pd
import boto3

class DataExtractor:
    @staticmethod
    def from_redshift_to_df(connection_string, query=None):
        if query is None:
            return sqlalchemy.create_engine(connection_string)
        else:
            engine = sqlalchemy.create_engine(connection_string)
            with engine.connect() as connection:
                data = pd.read_sql(query, connection)
            return data

    @staticmethod
    def from_synapse_to_df(connection_string, query=None):
        if query is None:
            raise ValueError('Necessário inserir uma query')
        else:
            with connection_string as connection:
                cursor = connection.cursor()
                cursor.execute(query)
                if cursor.description is not None:
                    columns = [column[0] for column in cursor.description]
                    rows = [dict(zip(columns, row))
                            for row in cursor.fetchall()]
                    df = pd.DataFrame(rows)
                    return df
                else:
                    raise ValueError('Nenhum dado encontrado.')

    @staticmethod
    def from_sqlserver_to_df(connection_string, query=None):
        if query is None:
            raise ValueError('Necessário inserir uma query')
        else:
            with connection_string as connection:
                cursor = connection.cursor()
                cursor.execute(query)
                if cursor.description is not None:
                    columns = [column[0] for column in cursor.description]
                    rows = [dict(zip(columns, row))
                            for row in cursor.fetchall()]
                    df = pd.DataFrame(rows)
                    return df
                else:
                    raise ValueError('Nenhum dado encontrado.')

    @staticmethod
    def from_postgresql_to_df(connection_string, query=None):
        if query is None:
            raise ValueError('Necessário inserir uma query')
        else:
            with connection_string as connection:
                cursor = connection.cursor()
                cursor.execute(query)
                if cursor.description is not None:
                    columns = [column.name for column in cursor.description]
                    rows = [dict(zip(columns, row))
                            for row in cursor.fetchall()]
                    df = pd.DataFrame(rows)
                    return df
                else:
                    raise ValueError('Nenhum dado encontrado.')

    @staticmethod
    def from_csv_to_df(file_path):
        if not file_path.endswith('.csv'):
            raise ValueError('O arquivo precisa ser um CSV.')

        with open(file_path, encoding='iso-8859-1') as csv_file_obj:
            reader = csv.reader(csv_file_obj, delimiter=',')
            columns = next(reader)
            data = [dict(zip(columns, row)) for row in reader]
            df = pd.DataFrame(data)
            df = df.rename(columns=lambda x: x.strip().replace(' ', '_'))
        return df

    @staticmethod
    def from_excel_to_df(file_path, sheet_name):
        wb = openpyxl.load_workbook(file_path)
        sheet = wb[sheet_name]
        rows = sheet.values
        columns = next(rows)
        data = [dict(zip(columns, row)) for row in rows]
        df = pd.DataFrame(data)
        df = df.rename(columns=lambda x: x.strip().replace(' ', '_'))
        return df

    @staticmethod
    def from_s3_to_df(s3_bucket, s3_key, aws_access_key_id=None, aws_secret_access_key=None):
        if s3_bucket is None or s3_key is None:
            raise ValueError('Bucket e chave do S3 são obrigatórios.')

        s3_resource = boto3.resource(
            's3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        obj = s3_resource.Object(s3_bucket, s3_key)
        content = obj.get()['Body'].read()
        df = pd.read_csv(io.StringIO(content.decode('utf-8')), sep=",")
        return df

    @staticmethod
    def from_mysql_to_df(connection_string, query=None):
        if query is None:
            raise ValueError('Necessário inserir uma query')
        else:
            df = pd.read_sql_query(query, connection_string)
            return df
