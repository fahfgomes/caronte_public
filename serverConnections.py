import os
import psycopg2
import pyodbc
from sqlalchemy import create_engine
from dotenv import load_dotenv
import time

load_dotenv()

def connect_to_sql_server(config):
    return pyodbc.connect(f"Driver={{{config['driver']}}};Server={config['server']};Database={config['database']};UID={config['username']};PWD={config['password']};")
    #return pyodbc.connect(f"mssql+pyodbc://{config['username']}:{config['password']}@{config['server']}/{config['database']}?driver={config['driver']}")
    
def connect_to_postgres(config):
    return psycopg2.connect(f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}")


def connect_to_mysql(config):
    engine = create_engine(f"mysql+pymysql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}")
    return engine.connect()

def get_database_config(name):
    configurations = {
        'JUNDSOFT': {
            'server': '',
            'database': '',
            'username': os.getenv("COMMON_USERNAME"),
            'password': os.getenv("COMMON_PASSWORD"),
            'driver': 'ODBC Driver 17 for SQL Server'
        },
        'JUNDSOFT_HML': {
            'server': '',
            'database': '',
            'username': os.getenv("COMMON_USERNAME"),
            'password': os.getenv("COMMON_PASSWORD"),
            'driver': 'SQL Server'
        },
        'Shefield_Tim': {
            'server': '',
            'database': '',
            'username': os.getenv("COMMON_USERNAME"),
            'password': os.getenv("COMMON_PASSWORD"),
            'driver': 'ODBC Driver 17 for SQL Server'
        },
        'Shefield_utilitario': {
            'server': '',
            'database': '',
            'username': os.getenv("COMMON_USERNAME"),
            'password': os.getenv("COMMON_PASSWORD"),
            'driver': 'ODBC Driver 17 for SQL Server'
        },
        'DW': {
            'host': '',
            'port': '',
            'database': '',
            'user': os.getenv("COMMON_USERNAME"),
            'password': os.getenv("COMMON_PASSWORD")
        },
        'DW-REDSHIFT': {
            'host': '',
            'port': '',
            'database': '',
            'user': os.getenv("REDSHIFT_DW_LOGIN"),
            'password': os.getenv("COMMON_PASSWORD")
        },
        'celulardireto_magento_prod':{
            'host': '',
            'database': '',
            'user': os.getenv("COMMON_USERNAME"),
            'password': os.getenv("COMMON_PASSWORD"),
            'port':''
        },
        'celulardireto_magento_qa01':{
            'host': '',
            'database': '',
            'user': os.getenv("COMMON_USERNAME"),
            'password': os.getenv("COMMON_PASSWORD"),
            'port':''
        },
        'hub_colmeia':{
            'host': '',
            'database': '',
            'user': os.getenv("COMMON_USERNAME"),
            'password': os.getenv("COMMON_PASSWORD"),
            'port':''
        },
        'desbloqueados_prod':{
            'host': '',
            'database': '',
            'user': os.getenv("COMMON_USERNAME"),
            'password': os.getenv("COMMON_PASSWORD"),
            'port':''
        },
        'corp-api-tim':{
            'host': '',
            'database': '',
            'user': os.getenv("COMMON_USERNAME"),
            'password': os.getenv("COMMON_PASSWORD"),
            'port':''
        },
        'AlliedKnox': {
            'host': '',
            'database': '',
            'user': os.getenv("COMMON_USERNAME"),
            'password': os.getenv("COMMON_PASSWORD"),
            'port': ''
        },
        'Soudi_Analytics': {
            'host': '',
            'database': '',
            'user': os.getenv("COMMON_USERNAME"),
            'password': os.getenv("COMMON_PASSWORD"),
            'port': ''
        },
        'Base_Emissor': {
            'host': '',
            'database': '',
            'user': os.getenv("BASE_EMISSOR_USER"),
            'password': os.getenv("BASE_EMISSOR_PASSWORD"),
            'port': ''
        },
        'backendecommerce': {
            'host': '',
            'database': '',
            'user': os.getenv("COMMON_USERNAME"),
            'password': os.getenv("COMMON_PASSWORD"),
            'port': ''
        },
        'Trade-in-collect': {
            'host': '',
            'database': '',
            'user': os.getenv("COMMON_USERNAME"),
            'password': os.getenv("COMMON_PASSWORD"),
            'port': ''
        },
        'Trade-in-recertification': {
            'host': '',
            'database': '',
            'user': os.getenv("COMMON_USERNAME"),
            'password': os.getenv("COMMON_PASSWORD"),
            'port': ''
        },
        'Trade-in-collect-varejo': {
            'host': '',
            'database': '',
            'user': os.getenv("COMMON_USERNAME"),
            'password': os.getenv("COMMON_PASSWORD"),
            'port': ''
        },
        'Trade-in-recertification-varejo': {
            'host': '',
            'database': '',
            'user': os.getenv("COMMON_USERNAME"),
            'password': os.getenv("COMMON_PASSWORD"),
            'port': ''
        },        
        'Atikas': {
            'server': '',
            'database': '',
            'username': os.getenv("COMMON_USERNAME"),
            'password': os.getenv("COMMON_PASSWORD"),
            'driver': 'ODBC Driver 17 for SQL Server'
    },
        'Sinc': {
            'host': '',
            'database': '',
            'user': os.getenv("COMMON_USERNAME"),
            'password': os.getenv("COMMON_PASSWORD"),
            'port': ''
    },
        'wz_pedidos_tradein': {
            'host': '',
            'database': '',
            'user': os.getenv("COMMON_USERNAME"),
            'password': os.getenv("COMMON_PASSWORD"),
            'port': ''
    },

        'CARE': {
            'host': '',
            'database': '',
            'user': os.getenv("CARE_USERNAME"),
            'password': os.getenv("CARE_PASSWORD"),
            'port': ''
    }

}
    if name in configurations:
        return configurations[name]

    raise ValueError(f"Banco de dados '{name}' não está configurado.")

def database(name, max_attempts=3, wait_time=5):
    for attempt in range(1, max_attempts + 1):
        try:
            config = get_database_config(name)
            if name in ['JUNDSOFT', 'JUNDSOFT_HML' , 'Shefield_Tim', 'Shefield_utilitario', 'Atikas']:
                connection = connect_to_sql_server(config)
            elif name in ['Base_Emissor', 'DW', 'DW-REDSHIFT']:
                connection = connect_to_postgres(config)
            else:
                connection = connect_to_mysql(config)
            return connection
        except Exception as e:
            if attempt < max_attempts:
                print(f"Tentativa {attempt} falhou. Aguardando {wait_time} segundos antes de tentar novamente.")
                time.sleep(wait_time)
            else:
                raise e

    raise ValueError(f"Todas as {max_attempts} tentativas de conexão falharam. Verifique suas configurações ou entre em contato com o Suporte e tente novamente mais tarde.")
