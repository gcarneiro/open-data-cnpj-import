import pymysql
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine
import urllib.parse


def iniciar_db(user, password, host, port, database):
    Base = automap_base()
    
    password = urllib.parse.quote_plus(password)
    
    # engine, suppose it has two tables 'user' and 'address' set up
    engine = create_engine(f"mariadb+pymysql://%s:%s@%s:%s/%s?charset=utf8mb4" % (user, password, host, port, database))

    # reflect the tables
    Base.prepare(engine, reflect=True)
    Arquivos_Processados = Base.classes.arquivos_processados

    return (engine, Arquivos_Processados)