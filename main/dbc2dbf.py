"""Modulo dbc2cbf - contem as funções:\n
dbc2dbf: converte arquivos DBC para DBF, deixando-os na mesma pasta.
"""

import os
from os import listdir
import sys
from query_execute import query_historico
import time
from lista_alvos import lista_alvos

def down_and_cc_github_blast_dbf():
    print("""   execute os seguintes comandos para baixar e compilar o blast-dbf.exe:
    apt install wget unzip gcc
    wget https://github.com/eaglebh/blast-dbf/archive/refs/heads/master.zip --no-check-certificate
    unzip master.zip
    cd blast-dbf-master
    cc -o blast-dbf.exe blast.c blast-dbf.c""")
    return False

def dbc2dbf(pasta_alvo):
    arquivos_alvo = lista_alvos(pasta_alvo)# chama a função para listar os arquivos da pasta_alvo
    caminho_local_atual = os.path.dirname(__file__)
    blast_dbf = 'blast-dbf.exe'
    if blast_dbf not in listdir(caminho_local_atual): # verifica se o executavel está na pasta
        print("{} não está na pasta atual".format(blast_dbf))
        down_and_cc_github_blast_dbf()
        exit(1)

    caminho_pasta_alvo = os.path.join(caminho_local_atual, pasta_alvo)
    historico_acoes = []
    for subpasta in arquivos_alvo:
        for arquivo in arquivos_alvo[subpasta]:
            caminho_blast_dbf = os.path.join(caminho_local_atual, blast_dbf)
            caminho_input_file = os.path.join(caminho_pasta_alvo, subpasta, arquivo)
            caminho_output_file = caminho_input_file.replace('.dbc', '.dbf')
            try:
                os.system("{} {} {}".format(caminho_blast_dbf, caminho_input_file, caminho_output_file))
            except Exception as err:
                print("Arquivo {} não convertido. Erro {}, {}".format(arquivo, sys.exc_info(), err),end ='')
                try:
                    os.remove(caminho_output_file)
                except Exception as err:
                    print("DBF não foi criado {}".format(err))
                else:
                    print("DBF removido")
                exit(1)
            else:
                nome_modulo = os.path.basename(__file__).split('.')[0]
                date_modified = os.path.getmtime(caminho_input_file)
                timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(date_modified))
                horario_execucao = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time()-3600*3))
                historico_acoes.append([arquivo, timestamp, horario_execucao,'dbc2dbf_convertido', nome_modulo])
                os.utime(caminho_output_file, (date_modified, date_modified))
                os.remove(caminho_input_file)
                historico_acoes.append([arquivo, timestamp, horario_execucao,'dbc_removido', nome_modulo])
                print("Arquivo {} descompactado e removido".format(arquivo))

    query_historico(historico_acoes)
    
if __name__ == "__main__":
    print("Testando as funções do modulo dbc2dbf.py")
    pasta_alvo ="arquivos"    
    dbc2dbf(pasta_alvo)