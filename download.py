import os, requests, datetime, threading
from bs4 import BeautifulSoup
from tqdm import tqdm
from db_main import *


BASEDIR = os.getcwd()
DOWNLOAD_URL = 'http://200.152.38.155/CNPJ/'
DOWNLOAD_DIR = f'{BASEDIR}/data/download'
EXTRACT_DIR = f'{BASEDIR}/data/output-extract'
LOG_FILE = f'{BASEDIR}/log.json'

#
def pegar_urls_no_site(url: str, ext: str = ''):
    """ Função responsavel por recuperar as urls dentro do parent folder """
    response = requests.get(url)

    if response.ok:
        response_text = response.text
    else:
        return response.raise_for_status()

    soup = BeautifulSoup(response_text, 'html.parser')
    parent = [{
        'nome': node.get('href'), 
        'url': url + node.get('href'), 
        'data_de_criacao': datetime.datetime.fromisoformat(node.parent.find_next_sibling().text.rstrip()),
        'data_de_processamento': datetime.datetime.now()
    } for node in soup.find_all('a') if node.get('href').endswith(ext)]

    return parent

def verificar_pasta_download():
    """ Função responsavel por verificar se as pastas necessarias existem, se não criar elas """
    if not os.path.exists(DOWNLOAD_DIR):
        os.mkdir(DOWNLOAD_DIR)

    resultados = pegar_urls_no_site(DOWNLOAD_URL, 'zip')
    session = Session(engine)

    for arquivo in resultados:
        novo_arquivo = session.query(Arquivos_Processados).filter_by(nome=arquivo['nome'], data_de_criacao=arquivo['data_de_criacao']).first()

        if not novo_arquivo:
            # Esse arquivo ainda não foi adicionado no banco
            arquivo['tamanho_total'] = 0
            arquivo['concluido'] = False

            novo_arquivo = Arquivos_Processados(**arquivo)

            session.add(novo_arquivo)
            session.commit()

        if novo_arquivo.concluido == False: # O arquivo ainda não foi totalmente baixado mais já foi adicionado no banco
            criar_thread_de_download(novo_arquivo.__dict__) 
        """ else:
            print(f'{novo_arquivo.nome}: Finalizado') """

        session.close()

def criar_thread_de_download(query):
    download_thread = threading.Thread(target=baixar_arquivo, args=[query])
    download_thread.start()

def baixar_arquivo(novo_arquivo):
    """ Função responsavel por baixar os dados do arquivo """
    arquivo_path = f'{DOWNLOAD_DIR}/{novo_arquivo["nome"]}'
    tamanho_atual = 0
    data = b''

    try:
        session = Session(engine)

        if os.path.exists(arquivo_path):
            tamanho_atual = os.stat(arquivo_path).st_size # Pega o tamanho do arquivo atual em bytes
            with open(arquivo_path, 'rb') as arquivo:
                data = arquivo.read()
                
        print(tamanho_atual)
        header = {"Range": f"bytes={tamanho_atual}-"}
        res = requests.get(novo_arquivo['url'], stream=True, headers=header)
        
        tamanho_total = int(res.headers.get('content-length', 0))
        progress_bar = tqdm(total=tamanho_total, unit='iB', unit_scale=True)

        with open(arquivo_path, 'ab') as arquivo:
            for chunk in res.iter_content(chunk_size=1024):
                if(chunk):
                    progress_bar.update(len(chunk))
                    arquivo.write(chunk)
        
        progress_bar.close()

        session.query(Arquivos_Processados).filter_by(nome=novo_arquivo['nome']).update({"concluido": True, "tamanho_total": len(data)})
        session.commit()
        session.close()

    except Exception as e:
        print(e)

def descompactar_dados():
    """  Função responsavel por descompactar os arquivos baixados """
    pass


def importar_mysql():
    """ Função responsavel por abrir os dados baixados e importas eles para o banco mysql """
    pass



verificar_pasta_download()