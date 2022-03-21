import sys, os, zipfile
from db_main import *
from parser.parsers import generate_parsers_from_files, EstabeleCsvParser
from parser.csv_reader import CsvReader
from parser.importer import SqlImport, MysqlImport
from tools.log import Log

from tqdm import tqdm

BASEDIR = os.getcwd()


BASEDIR = os.getcwd()
DOWNLOAD_DIR = f'{BASEDIR}/data/download'
DEFAULT_DIR = f'{BASEDIR}/data/output-extract'

log = Log()

log.info('Analyzing files')


filenames = next(os.walk(DOWNLOAD_DIR), (None, None, []))[2]  # [] if no file

#print(filenames)


session = Session(engine)
arquivos = session.query(Arquivos_Processados).filter_by(concluido=True).all()

""" for arquivo in arquivos:
    arquivo_path = f'{DOWNLOAD_DIR}/{arquivo.nome}'

    if os.path.exists(arquivo_path):
        with zipfile.ZipFile(arquivo_path, 'r') as zip_ref:
            zip_ref.extractall(DEFAULT_DIR)
 """
session.close()

parsers = generate_parsers_from_files(DEFAULT_DIR, log)

def build_insert(parser, keys):
        print(keys)
        """ sqlKeys = ','.join(keys)
        sqlValues = ','.join(['%s'] * len(keys))
        """
        
        #print(sqlValues)
        #return f'INSERT INTO {parser.TABLE} ({sqlKeys}) VALUES ({sqlValues}) ON DUPLILCATE KEY UPDATE '



def run(parser, limit=0):
        lines = []
        keys = []
        pbar = tqdm(total=parser.get_size())
        count = 0

        while limit == 0 or count <= limit:
            lines = parser.parse_bulk(5000)
            count += len(lines)
            if len(lines) == 0:
                break

            try:
                lines_in_tuples = list(map(lambda line: tuple(line.values()), lines))
                keys = lines[0].keys()
                print(build_insert(parser, keys), lines_in_tuples)
                pbar.update(len(lines))
            except Exception as e:
                print(e)

            lines = []

count = 0
for parser in parsers:
    log.info('Importing file', parser.get_name(), '-', count + 1, 'of', len(parsers))
    run(parser)
    count += 1