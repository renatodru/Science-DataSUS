"""Modulo down_ftp - contém as funções:\n
local_do_arquivo: cria dentro do diretório atual a pasta e subpastas especificadas para que os arquivos sejam baixados de forma organizada.\n
gera_arquivos_ftp: após o usuário especificar os arquivos desejados, a função gera os nomes dos arquivos para download e as retorna.\n
baixaftp: recebe os nomes dos arquivos e faz o download para as pastas especificadas.
"""
import os
from ftplib import FTP, all_errors
from query_execute import query_historico
import time

def local_do_arquivo(nome_pasta, subpastas):
    local_atual = os.path.dirname(__file__)
    caminhos_pastas = []
    caminhos_pastas.append(os.path.join(local_atual, nome_pasta))
    [caminhos_pastas.append(os.path.join(caminhos_pastas[0], subpasta)) for subpasta in subpastas]    
    for caminho in caminhos_pastas:
        try:
            if not os.path.exists(caminho):
                os.makedirs(caminho)
                print("PASTA {} CRIADA COM SUCESSO".format(caminho))
            else:
                print("PASTA '{}' JÁ EXISTE".format(caminho))
                continue
        except OSError as err:
            print("ERRO AO CRIAR PASTA {} : {}".format(caminho, err))
            return False
        else:
            continue
    return True

def validador(data_inicial, data_final, estado, tipo):
    #cadastros permitidos
    estados = ['PR', 'RS', 'SC', 'SE', 'TO', 'AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT', 'PA', 'PB', 'PE', 'PI', 'RJ', 'RN', 'RO', 'RR', 'SE', 'SP', 'TO']
    tipos = ['RD', 'SP', 'PA', 'RJ', 'ER']
    
    #validação de entrada
    for tip in tipo:
        if tip not in tipos:
            print("TIPO DE ARQUIVO {} INEXISTENTE, TIPOS POSSIVEIS: {}".format(tipo, tipos))
            exit(1)
    for estad in estado:
        if estad not in estados:
            print("ESTADO {} ESTÁ INCORRETO, ESCOLHA UM ENTRE OS SEGUINTES: {}".format(estado, estados))
            exit(1)
        
    #validação de datas
    data_inicial, data_final = data_inicial.split('/')[:2], data_final.split('/')[:2]
    if len(data_inicial[0]) != 4 or len(data_final[0]) != 4:#verifica se o ano de inicio é maior que o ano de fim
        print("DATA INFORMADA NÃO É VALIDA - AAAA/MM")
        exit(1)
    if int(data_inicial[0]) > int(data_final[0]):#uma vez verificado o ano, verifica se o mes de inicio é maior que o mes de fim
        print("DATA INFORMADA NÃO É VALIDA - AAAA/MM")
        exit(1)
    return True

def gera_arquivos_ftp(data_inicial, data_final, estado, tipo):
    try:
        validador(data_inicial, data_final, estado, tipo)
    except Exception as err:
        print("ERRO AO VALIDAR DADOS: {}".format(err))
        exit(1)    

    data_inicial, data_final = data_inicial.split('/')[:2], data_final.split('/')[:2]
   
    #montagem dos nomes dos arquivos, gera todas as combinações possíveis
    arquivos = []
    for tip in tipo:
        for estad in estado:
            final = tip+estad+data_final[0][2:4]+data_final[1]+'.dbc'# NOME COMPLETO DO ARQUIVO NO DATASUS            
            mes = int(data_inicial[1])
            ano = int(data_inicial[0][2:4])
            while final not in arquivos:# gera os nomes de todos os arquivos respeitando a ordem de ano e mes
                if len(str(mes)) == 1:
                    mes = '0'+str(mes)#garante o formato de 2 digitos para o mes
                arquivos.append(tip+estad+str(ano)+str(mes)+'.dbc')
                if mes == 12:#garante que o mes seja reiniciado ao final de cada ano
                    mes = 1
                    ano += 1
                    continue
                mes = int(mes)+1
    return arquivos

# recebe os nomes dos arquivos e faz o download
def down_arq_ftp(data_inicial, data_final, estado, tipo, nome_pasta="arquivos"):
    pastas_datasus = {'SP': '/dissemin/publicos/SIHSUS/200801_/Dados/',
                      'RD': '/dissemin/publicos/SIHSUS/200801_/Dados/',
                      'PA': '/dissemin/publicos/SIASUS/200801_/Dados/',
                      'rotulos': ''}
    try:
        bancos = gera_arquivos_ftp(data_inicial, data_final, estado, tipo)
    except OSError as err:
        print("ERRO AO GERAR NOMES DE ARQUIVOS: {}".format(err))
        return False
    if local_do_arquivo(nome_pasta, pastas_datasus.keys()):
        print("ARQUIVOS PRONTOS PARA DOWNLOAD {}".format(bancos))   
        with FTP('ftp.datasus.gov.br') as ftp:# aponta o servidor
            lista_downloads = []
            for banco in bancos:
                alvo = pastas_datasus[banco[:2]]+banco
                try:
                    ftp.login()  # faz login anonimo padrao
                    arquivo_local = os.path.join(os.path.dirname(__file__), nome_pasta, banco[:2], banco)
                    with open(arquivo_local, 'wb') as localfile:#cria o arquivo local
                        ftp.retrbinary('RETR ' + alvo, localfile.write)#download do arquivo remoto para dentro do arquivo local
                        mdtm = ftp.sendcmd('MDTM ' + alvo)#pega a data de modificação do arquivo no servidor
                        timestamp = int(time.mktime(time.strptime(mdtm[4:].strip(), "%Y%m%d%H%M%S")))
                    os.utime(arquivo_local, (timestamp, timestamp)) #atualiza a data de modificação do arquivo para a data original
                except all_errors as err:
                    print('FTP error:', err)
                    os.remove(localfile.name)
                except OSError:
                    print("Erro ao alterar data de modificação do arquivo: {}".format(OSError))
                else:
                    print("Arquivo {} baixado com sucesso em {}".format(alvo, arquivo_local))
                    nome_modulo = os.path.basename(__file__).split('.')[0]
                    horario_execucao = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time()-3600*3))
                    lista_downloads.append([banco, mdtm[4:].strip(), horario_execucao, "download-dbc", nome_modulo])
            query_historico(lista_downloads)
        return True
    exit(1)


if __name__ == "__main__":
    #testa funções basicas do modulo
    print("testando o modulo down_ftp.py")
    data_inicial = "2021/01"
    data_final = "2021/02"
    estado = ['PR','SC']
    tipo = ['RD','SP']
    down_arq_ftp(data_inicial, data_final, estado, tipo)