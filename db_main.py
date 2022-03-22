import pymysql
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine

def iniciar_db(user, password, host, port, database):
    Base = automap_base()
    
    # engine, suppose it has two tables 'user' and 'address' set up
    engine = create_engine(f"mariadb+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4")

    # reflect the tables
    Base.prepare(engine, reflect=True)
    Arquivos_Processados = Base.classes.arquivos_processados

    return (engine, Arquivos_Processados)