import pymysql
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

Base = automap_base()

# engine, suppose it has two tables 'user' and 'address' set up
engine = create_engine("mariadb+pymysql://admin:1234@localhost/cnpjimport?charset=utf8mb4")

# reflect the tables
Base.prepare(engine, reflect=True)

# mapped classes are now created with names by default
# matching that of the table name.
Cnae = Base.classes.cnae
Empresa = Base.classes.empresa
Arquivos_Processados = Base.classes.arquivos_processados


#print(session.query(Arquivos_Processados).all())