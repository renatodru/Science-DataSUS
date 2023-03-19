"""Modulo carrega_df - contem a função:
carrega_df(subpasta, arquivo, pasta_alvo = 'arquivos')
carrega um arquivo dbf ou xlsx e retorna um dataframe pandas.
subpasta = subpasta da pasta alvo, no caso padrão é 'arquivos'
arquivo = nome do arquivo que será carregado
"""

import os
import sys

import datetime #para contar o tempo de carregamento
import time #para medir o tempo do algoritmo

#from simpledbf import Dbf5
import pandas as pd
import psycopg
from cnx_db_tables import conectar

from query_execute import query_historico

#import mysql.connector
#from mysql.connector import errorcode
#from lista_alvos import lista_alvos
#from dbf import Table

def carrega_df(subpasta, arquivo, pasta_alvo = 'arquivos'):
    inicio = datetime.datetime.now()#inicio da contagem de tempo
    local = os.path.dirname(__file__)    
    caminho_arquivo = os.path.join(local,pasta_alvo,subpasta,arquivo)
    if subpasta == 'rotulos' and arquivo[-3:] == 'dbf':#define a variavel codec para carregamento correto do arquivo dbf
        codec_tipo='cp1250'
    else:
        codec_tipo='cp850'   
    try:#carrega o arquivo dbf e já o converte para dataframe 
        if arquivo[-3:] == 'dbf':
            dataframe = Dbf5(caminho_arquivo, codec=codec_tipo).to_dataframe(na='')
        elif arquivo[-4:] == 'xlsx':
            dataframe = pd.read_excel(caminho_arquivo)
    except sys.exc_info() as err:
        print("ERRO COM O ARQUIVO '{}': MENSAGEM: {}".format(caminho_arquivo, err))
        dataframe = None    
    else:
        print("CARREGAMENTO DO ARQUIVO '{}' CONCLUIDO EM {}".format(arquivo, datetime.datetime.now()-inicio))
    return dataframe


def lista_alvos(pasta_alvo):
    dict_pasta_dbf_existente = {}
    caminho_atual = os.path.dirname(__file__)
    pasta_atual = os.path.join(caminho_atual,pasta_alvo)
    subpastas_nome = [f.name for f in os.scandir(pasta_atual) if f.is_dir()]
    
    for subpasta in subpastas_nome:
        lista_arq = [arq for arq in os.listdir(os.path.join(pasta_atual,subpasta)) if arq[-3:]=="dbf"]
        if len(lista_arq)>0:
            dict_pasta_dbf_existente.update({subpasta:lista_arq})
    print(dict_pasta_dbf_existente)
        
    #historico = [lista_h[:-1] for lista_h in open(caminho_atual+"DBF_importados_MySQL.txt",'r').readlines()]
    #excluir = []
    #for x in dict_pasta_arquivo:
        # for h in historico:
        #     if h in dict_pasta_arquivo[x]:
        #         dict_pasta_arquivo[x].remove(h)
        #if(len(dict_pasta_arquivo[x])==0):
            #excluir.append(x)
    #if(len(excluir)>0):
        #for exc in excluir:
            #del dict_pasta_arquivo[exc]    

    #return dict_pasta_arquivo
    
def time_stamp_file(caminho_arquivo):
    date_modified = os.path.getmtime(caminho_arquivo)
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(date_modified))


def abre_dbf(subpasta,dbf_alvo,pasta_alvo):#devolve o arquivo como um objeto table do dbf
    localatual = os.path.dirname(__file__)+'\\{}\\'.format(pasta_alvo)
    dbf_file = localatual+subpasta+"\\"+dbf_alvo
    return Table(dbf_file).open() #atribui o DBF à variavel, a abre e retorna como chamado da função
    #table_ventas = DBF(f'{table_name}', load=True,ignore_missing_memofile=True)


def escreve_bd(cnx,dict_alvos,pasta_alvo):
    try:
        cursor = cnx.cursor()
    except:
        print("Erro definindo cursor")
    database = 'DATASUS'
    cursor.execute("USE {}".format(database))
    #print("Alvos para importação {}".format(dict_alvos))
    cursor.execute('SET GLOBAL max_allowed_packet=500*1024*1024')
    cursor.execute('SET GLOBAL wait_timeout = 28800')
    #cursor.execute("DELETE FROM PAPR")
    cursor.fast_execute = True
    cnx.commit()
    for pasta in dict_alvos:
        table = pasta
        for arq in dict_alvos[pasta]:
            inicio = time.time()
            dbf_file = abre_dbf(pasta,arq,pasta_alvo)
            linhas,colunas = len(dbf_file),dbf_file.field_count
            print("{} aberto:(Linhas:{} x Colunas: {}), tempo load: {}".format(arq, linhas, colunas,time.time()-inicio))
            stmt = "INSERT INTO {} {} VALUES ({}{})".format(pasta, str(tuple(dbf_file.field_names)).replace("'", ""), "%s, "*(dbf_file.field_count-1), "%s" )
            inicio = time.time()
            passo = 50000
            for i in range(0, linhas, passo):
                try:cursor.executemany(stmt, dbf_file[i:i + passo])
                except mysql.connector.Error as err:
                    print(err)
                    cursor.close()
                    cnx.close()
                    return
                else:print("Query executada, Tempo: {}, realizado: %{}".format(time.time()-inicio,100*(i+passo)/linhas))
                        
            try:cnx.commit()
            except mysql.connector.Error as err:
                print(err)
                return
            else:
                print("Commit executado",end ='')
                try:
                    salva_h(arq)
                except:
                    print("Erro salvando {} no historico de importação".format(arq))
                    cursor.close()
                    cnx.close()
                else:
                    print("Salvo no Historico de importação.")

                
    cursor.close()
    cnx.close()


#alterar para o postgresql usando o modulo query_execute
#COMPETENCIA, DATA_MODIFICACAO_ARQ, DATA_OPERACAO, OPERACAO, MODULO_EXECUTANTE
def salva_h(y):
    lista_downloads = []
    nome_modulo = os.path.basename(__file__).split('.')[0]
    horario_execucao = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time()-3600*3))
    
    lista_downloads.append([banco, mdtm[4:].strip(), horario_execucao, "importa_dbf_pgsql", nome_modulo])
    query_historico(lista_downloads)




#lista_alv = {"PAPR":["PAPR2101.dbf"]}#,"SPPR2102.dbf","SPPR2103.dbf","SPPR2104.dbf","SPPR2105.dbf","SPPR2106.dbf","SPPR2107.dbf","SPPR2108.dbf","SPPR2109.dbf"]} #dbf para teste

#pasta_alvo="DADOS"

#escreve_bd(conecta_db(),lista_alv,pasta_alvo)

#escreve_bd(conecta_db(),lista_alvos(pasta_alvo),pasta_alvo)

if __name__ == "__main__":
    print("Testando modulo dbf2pgsql")
    subpasta = 'RD'  
    #print(pd.concat([carrega_df(subpasta, 'RDPR2111.dbf'), carrega_df(subpasta, 'RDPR2112.dbf')]))
    print(lista_alvos("arquivos"))
    #print(carrega_df('rotulos', 'municipios.xlsx'))