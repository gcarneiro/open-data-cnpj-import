import os, requests, datetime, threading, zipfile, traceback
from bs4 import BeautifulSoup
from tqdm import tqdm
from db_main import *

THREADS = []
BASEDIR = os.getcwd()
DOWNLOAD_URL = 'http://200.152.38.155/CNPJ/'
DOWNLOAD_DIR = f'{BASEDIR}/data/download'
EXTRACT_DIR = f'{BASEDIR}/data/output-extract'
LOG_FILE = f'{BASEDIR}/log'


def pegar_urls_no_site(url: str, ext: str = ''):
    """ Função responsavel por recuperar as urls de download no servidor """
    response = requests.get(url)

    if response.ok:
        response_text = response.text
    else:
        return response.raise_for_status()

    soup = BeautifulSoup(response_text, 'html.parser')
    resultado = [{
        'nome': node.get('href'), 
        'url': url + node.get('href'), 
        'data_de_criacao': datetime.datetime.fromisoformat(node.parent.find_next_sibling().text.rstrip()),
        'data_de_processamento': datetime.datetime.now()
    } for node in soup.find_all('a') if node.get('href').endswith(ext)]

    return resultado

def verificar_pasta_iniciar_download():
    """ Função responsavel por verificar se as pastas necessarias existem, e iniciar o download"""
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

    session.close()

def criar_thread_de_download(query):
    THREADS.append(threading.Thread(target=baixar_arquivo, args=[query]))

def baixar_arquivo(novo_arquivo):
    """ Função responsavel por baixar os dados do arquivo """
    arquivo_path = f'{DOWNLOAD_DIR}/{novo_arquivo["nome"]}'
    session = Session(engine)

    try:
        header_res = requests.head(novo_arquivo['url'])
        tamanho_total = int(header_res.headers.get('content-length', 0)) # Pega o tamanho total do arquivo em bytes pelo header da request

        if os.path.exists(arquivo_path):
            tamanho_atual = os.stat(arquivo_path).st_size # Pega o tamanho do arquivo atual em bytes
        else:
            open(arquivo_path, 'w+').close()
            tamanho_atual = 0

        if tamanho_atual < tamanho_total:
            header = {"Range": f"bytes={tamanho_atual}-"}
            res = requests.get(novo_arquivo['url'], stream=True, headers=header, timeout=60)
            progress_bar = tqdm(desc=novo_arquivo['nome'], total=tamanho_total-tamanho_atual, unit='iB', unit_scale=True)

            with open(arquivo_path, 'ab') as arquivo:
                for chunk in res.iter_content(chunk_size=1024):
                    if(chunk):
                        progress_bar.update(len(chunk))
                        arquivo.write(chunk)
            
            progress_bar.close()

        elif tamanho_atual == tamanho_total:
            print(f'Já baixado: {novo_arquivo["nome"]}')

        else:
            print('error inesperado')

        if verificar_arquivo_final(arquivo_path): # Caso o zip baixado seja valido
            session.query(Arquivos_Processados).filter_by(nome=novo_arquivo['nome']).update({
                "concluido": True, 
                "tamanho_total": tamanho_total
            })

            session.commit()
            print(f'{novo_arquivo["nome"]}: Finalizado com sucesso.')
        else: # se o zip não for valido
            if tamanho_atual >= tamanho_total: # Porem o tamanho do arquivo e maior ou igual o esperado, Remove o arquivo pois algo inesperado aconteceu
                os.remove(arquivo_path)
            
            print('tenta baixar novamente')
            baixar_arquivo(novo_arquivo) # Tenta baixar o arquivo novamente
    
    except requests.exceptions.ConnectionError as e:
        print('tenta baixar novamente')
        baixar_arquivo(novo_arquivo)

    except Exception as e:
        print(traceback.print_exc())

    finally:
        session.close()

def verificar_arquivo_final(arquivo_path):
    """ Essa função vai verificar se o arquivo final foi baixado corretamente """
    try:
        if not zipfile.ZipFile(arquivo_path).testzip():
            return True
        else:
            return False
    except zipfile.BadZipFile:
        return False


def criar_arquivo_lock():
    ''' adquire o acesso exclusivo da execução do script, caso outro processo já tenha adquirido sobre uma execeção '''
    if not os.path.exists(f'{BASEDIR}/.lockfile'):
        open('.lockfile', 'w+').close()
        return True
    elif get_data_modificacao() < (datetime.datetime.now() - datetime.timedelta(days = 2)):
        print('2 dias')
    else:
        return False

def remover_arquivo_lock():
    ''' libera o acesso da execução para outros processos '''
    os.remove(f'{BASEDIR}/.lockfile')

def get_data_modificacao(arquivo_path):
    t = os.path.getmtime(arquivo_path)
    return datetime.datetime.fromtimestamp(t)

try:
    if criar_arquivo_lock():
        verificar_pasta_iniciar_download()
        
        # Inicia todas as Threads
        for download in THREADS:
            download.start()

        # Espera todas as Threads acabarem antes de finalizar o script
        for download in THREADS:
            download.join()

except Exception as e:
    print(e)

finally:
    remover_arquivo_lock() # Ao finalizar a execução do script remover o arquivo lock