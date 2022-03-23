import os, requests, datetime, threading, zipfile, traceback, sys
from sqlalchemy.orm import Session
from bs4 import BeautifulSoup
from tools.log import Log
from tqdm import tqdm
from db_main import iniciar_db

log = Log()

THREADS = []
EXECUTANDO = False
BASEDIR = os.getcwd()
DOWNLOAD_URL = 'http://200.152.38.155/CNPJ/'
DOWNLOAD_DIR = f'{BASEDIR}/data/download'
EXTRACT_DIR = f'{BASEDIR}/data/output-extract'

def parse_args():
    if len(sys.argv) < 5:
        print('usage: mysql_import.py <host> <port> <user> <password> <database> <directory>')
        return False
    
    args = {
        'user': sys.argv[3],
        'password': sys.argv[4],
        'host': sys.argv[1],
        'port': sys.argv[2],
        'database': sys.argv[5]
    }

    return args

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

    log.info(f'{len(resultado)} encontrados no site')

    return resultado

def verificar_pasta_iniciar_download():
    """ Função responsavel por verificar se as pastas necessarias existem, e iniciar o download"""
    if not os.path.exists(DOWNLOAD_DIR):
        log.info(f'Criando DOWNLOAD_DIR.')
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

        #print(novo_arquivo.nome, novo_arquivo.concluido)
        if novo_arquivo.concluido == False: # O arquivo ainda não foi totalmente baixado mais já foi adicionado no banco
            criar_thread_de_download(novo_arquivo.__dict__)
        else:
            if os.path.exists(f'{DOWNLOAD_DIR}/{novo_arquivo.nome}'):
                log.info(f'{novo_arquivo.nome}: Arquivo já foi concluido')
            else:
                novo_arquivo = novo_arquivo.__dict__
                session.query(Arquivos_Processados).filter_by(nome=novo_arquivo['nome']).update({"concluido": False, 'tamanho_total': 0})
                session.commit()
                
                criar_thread_de_download(novo_arquivo)

    session.close()

def criar_thread_de_download(query):
    THREADS.append(threading.Thread(target=baixar_arquivo, args=[query]))

def baixar_arquivo(novo_arquivo):
    """ Função responsavel por baixar os dados do arquivo """
    arquivo_path = f'{DOWNLOAD_DIR}/{novo_arquivo["nome"]}'
    session = Session(engine)

    log.info(f'{novo_arquivo["nome"]}: Baixando Arquivo.')

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
        else:
            log.error(f'{novo_arquivo["nome"]}: Error inesperado.')

        if verificar_arquivo_final(arquivo_path): # Caso o zip baixado seja valido
            session.query(Arquivos_Processados).filter_by(nome=novo_arquivo['nome']).update({
                "concluido": True, 
                "tamanho_total": tamanho_total
            })

            session.commit()
            log.info(f'{novo_arquivo["nome"]}: Arquivo valido e concluido.')
        else: # se o zip não for valido
            if tamanho_atual >= tamanho_total: # Porem o tamanho do arquivo e maior ou igual o esperado, Remove o arquivo pois algo inesperado aconteceu
                log.error(f'{novo_arquivo["nome"]}: Arquivo invalido')
                os.remove(arquivo_path)
            
            log.info(f'{novo_arquivo["nome"]}: Tentando baixar novamente.')
            baixar_arquivo(novo_arquivo) # Tenta baixar o arquivo novamente
    
    except requests.exceptions.ConnectionError as e:
        log.error(f'{novo_arquivo["nome"]}: Error de conexão com internet')
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
    try:
        arquivo_path = f'{BASEDIR}/.lockfile'
        if not os.path.exists(arquivo_path):
            log.info(f'Criando arquivo lock')
            open('.lockfile', 'w+').close()
            return True
        elif get_data_modificacao(arquivo_path) < (datetime.datetime.now() - datetime.timedelta(days = 2)):
            log.info(f'Arquivo lock existe a 2 dias, excluindo ele')
            remover_arquivo_lock()
            return True
        else:
            log.info(f'Impossivel iniciar o script, outro script parece estar sendo executado nesse momento')
            return False
    except Exception as e:
        log.error(str(e))
        raise e

def remover_arquivo_lock():
    ''' libera o acesso da execução para outros processos '''
    os.remove(f'{BASEDIR}/.lockfile')

def get_data_modificacao(arquivo_path):
    t = os.path.getmtime(arquivo_path)
    return datetime.datetime.fromtimestamp(t)

def exportar_arquivos():
    """ Captura todos os arquivos concluidos no banco e exporta eles para a pasta designada """
    try:
        if not os.path.exists(EXTRACT_DIR):
            os.mkdir(EXTRACT_DIR)

        session = Session(engine)
        arquivos = session.query(Arquivos_Processados).filter_by(concluido=True).all()

        for arquivo in arquivos:
            log.info(f"Exportando arquivo: {arquivo.nome}")
            arquivo_path = f'{DOWNLOAD_DIR}/{arquivo.nome}'

            if os.path.exists(arquivo_path):
                with zipfile.ZipFile(arquivo_path, 'r') as zip_ref:
                    zip_ref.extractall(EXTRACT_DIR)
        
        pasta_nome = f'{DOWNLOAD_DIR}_concluido_{datetime.datetime.date().strftime("%y-%m-%d")}'
        log.info(f'Exporatação finalizada, renomeando pasta download para: {pasta_nome}')
        os.rename(DOWNLOAD_DIR, pasta_nome) # Muda o nome da pasta

    except Exception as e:
        log.error(str(e))
        raise e
    
    finally:
        session.close()

try:
    db_args = parse_args()
    

    if db_args:
        destravado = criar_arquivo_lock()
        if destravado:
            engine, Arquivos_Processados = iniciar_db(**db_args)
            verificar_pasta_iniciar_download()
            
            # Inicia todas as Threads
            for download in THREADS:
                download.start()

            # Espera todas as Threads acabarem antes de finalizar o script
            for download in THREADS:
                download.join()

            exportar_arquivos() # Exporta os arquivos concluidos na pasta ao finalizar exclui a pasta downloads
            remover_arquivo_lock() # Ao finalizar a execução do script remover o arquivo lock

except Exception as e:
    print(e)

finally:
    if destravado:
        remover_arquivo_lock() # Ao finalizar a execução do script remover o arquivo lock