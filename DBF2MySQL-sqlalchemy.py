import mysql.connector
from mysql.connector import errorcode


import os
from os import listdir
import sqlalchemy
import time #para medir o tempo do algoritmo

def conecta_db():
    #https://docs.sqlalchemy.org/en/14/dialects/mysql.html#module-sqlalchemy.dialects.mysql.mysqlconnector
    #mysql+mysqldb://<user>:<password>@<host>[:<port>]/<dbname>
    
    
    
    pass

def lista_alvos(pasta_alvo):
    dict_pasta_arquivo = {}
    caminho_atual = os.path.dirname(__file__)+"\\"
    pasta_atual = caminho_atual+pasta_alvo+"\\"
    subpastas_nome = [f.name for f in os.scandir(pasta_atual) if f.is_dir()]
    
    for subpasta in subpastas_nome:
        lista_arq = [arq for arq in listdir(pasta_atual+subpasta) if arq[-3:]=="dbf"]
        if len(lista_arq)>0:dict_pasta_arquivo.update({subpasta:lista_arq})
        
    historico = [lista_h[:-1] for lista_h in open(caminho_atual+"DBF_importados_MySQL.txt",'r').readlines()]
    excluir = []
    for x in dict_pasta_arquivo:
        for h in historico:
            if h in dict_pasta_arquivo[x]:dict_pasta_arquivo[x].remove(h)
        if(len(dict_pasta_arquivo[x])==0):excluir.append(x)
    if(len(excluir)>0):
        for exc in excluir: del dict_pasta_arquivo[exc]    
    return dict_pasta_arquivo

def abre_dbf(subpasta,dbf_alvo,pasta_alvo):#devolve o arquivo como um objeto table do dbf
    pass

def escreve_bd(cnx,dict_alvos,pasta_alvo):
    pass

def salva_h(y):
    arquivo = open(os.path.dirname(__file__)+"\\DBF_importados_MySQL.txt","a")
    arquivo.write(str(y)+'\n')
    arquivo.close()


pasta_alvo="DADOS"
#lista_alv = {"SPPR":["SPPR2102.dbf"]}#,"SPPR2102.dbf","SPPR2103.dbf","SPPR2104.dbf","SPPR2105.dbf","SPPR2106.dbf","SPPR2107.dbf","SPPR2108.dbf","SPPR2109.dbf"]} #dbf para teste
#escreve_bd(conecta_db(),lista_alv,pasta_alvo)
#print(lista_alvos(pasta_alvo))
escreve_bd(conecta_db(),lista_alvos(pasta_alvo),pasta_alvo)
