import os
import keyring
import getpass
from dotenv import load_dotenv      #from package python-dotenv

def get_config():

    config = {}
    try:
        load_dotenv()
        config["_db_user"] = os.getenv('DBUSER')        
        config["_db_pw"] = keyring.get_password(os.getenv("KEYRING_SERVICE"), os.getenv("KEYRING_USER"))
        config["_host"] = os.getenv('HOST')
        config["_port"] = os.getenv('PORT')
        config["_database"] = os.getenv('DATABASE')
        if config["_db_pw"]:
            print('Retrieved db credentials.')
    
        else:
            print('===No db credentials for PG===')
            intext = getpass.getpass('Enter db password now to store it in Windows Cred Mgr:')
            if intext:
                keyring.set_password(os.getenv("KEYRING_SERVICE"), os.getenv("KEYRING_USER"), intext)
                config["_db_pw"] = intext
    
        return config

    except Exception as e:
        print('Error establishing PG credentials: ', e)

def get_pg_conn():
    import psycopg2

    config = get_config()

    try:
        conn = psycopg2.connect(database=config["_database"]
                            , user=config["_db_user"]
                            , password=config["_db_pw"]
                            , host=config["_host"]
                            , port=config["_port"]
                            )
        print('Established database connection.')
    except Exception as e:
        print('Error creating PG connection: ', e)

# elif CONN_TYPE=='Mem':
#         try:
#             conn = sl.connect(':memory:')        
#         except Exception as e:
#             print('Error creating sqlite memory connection:', e)
# elif CONN_TYPE=='SQLITE':            
#         try:
#             conn = sl.connect(SQLITE3_DB_PATH)
#         except Exception as e:
#             print('Error creating sqlite memory connection:', e)

    return conn

def get_sqlalchemy_pg_engine():
    from sqlalchemy import create_engine

    config = get_config()

    try:
        engine = create_engine(f"postgresql+psycopg2://{config['_db_user']}:{config['_db_pw']}@{config['_host']}:{config['_port']}/{config['_database']}")
        print('Established database engine.')
        return engine
    
    except Exception as e:
        print('Error creating PG engine: ', e)

    