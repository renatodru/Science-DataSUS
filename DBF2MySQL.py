import mysql.connector
from mysql.connector import errorcode
import os
from os import listdir
from dbf import *
import time #para medir o tempo do algoritmo

def conecta_db():
    config = {'user':'root','password':'root','host':'localhost','port':'3306','raise_on_warnings':True}
    try:cnx = mysql.connector.connect(**config)#user='root',password='root',host='127.0.0.1',port='3306',raise_on_warnings=True
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:print("Senha ou usuario incorretos")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:print("Database não existe")
        else:print(err)
    else:print("Conectado em {}:{}, usuario: {}".format(config['host'],config['port'],config['user']))
    return cnx

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
    localatual = os.path.dirname(__file__)+'\\{}\\'.format(pasta_alvo)
    dbf_file = localatual+subpasta+"\\"+dbf_alvo
    return Table(dbf_file).open() #atribui o DBF à variavel, a abre e retorna como chamado da função
    #table_ventas = DBF(f'{table_name}', load=True,ignore_missing_memofile=True)

def escreve_bd(cnx,dict_alvos,pasta_alvo):
    try:cursor = cnx.cursor()
    except:print("Erro definindo cursor")
    database = 'DATASUS'
    cursor.execute("USE {}".format(database))
    #print("Alvos para importação {}".format(dict_alvos))
    cursor.execute('SET GLOBAL max_allowed_packet=500*1024*1024')
    cursor.execute('SET GLOBAL wait_timeout = 28800')
    cursor.execute("DELETE FROM SPPR")
    cursor.fast_execute = True
    cnx.commit()
    for pasta in dict_alvos:
        table = pasta
        for arq in dict_alvos[pasta]:
            dbf_file = abre_dbf(pasta,arq,pasta_alvo)
            print("{} aberto:(Linhas:{} x Colunas: {})".format(arq, len(dbf_file), dbf_file.field_count))
            stmt = "INSERT INTO {} {} VALUES ({} {})".format(pasta, str(tuple(dbf_file.field_names)).replace("'", ""), "%s, "*(dbf_file.field_count-1), "%s" )
            inicio = time.time()
            try:cursor.executemany(stmt, dbf_file);print("Tempo: ",time.time()-inicio)
            except mysql.connector.Error as err:print(err);cursor.close();cnx.close();return
            else:print("Query executada")

            
            try:cnx.commit()
            except mysql.connector.Error as err:print(err);return
            else:
                #print("Commit executado")
                try:salva_h(arq)
                except:print("Erro salvando {} no historico de importação".format(arq));cursor.close();cnx.close()
                #else:print("Salvo no Historico de importação.")

                
    cursor.close()
    cnx.close()

def salva_h(y):
    arquivo = open(os.path.dirname(__file__)+"\\DBF_importados_MySQL.txt","a")
    arquivo.write(str(y)+'\n')
    arquivo.close()

#print(lista_alvos("DADOS"))

lista_alv = {"SPPR":["SPPR2101.dbf","SPPR2102.dbf","SPPR2103.dbf","SPPR2104.dbf","SPPR2105.dbf","SPPR2106.dbf","SPPR2107.dbf","SPPR2108.dbf","SPPR2109.dbf"]} #dbf para teste
pasta_alvo="DADOS"
escreve_bd(conecta_db(),lista_alv,pasta_alvo)

#escreve_bd(conecta_db(),lista_alvos(pasta_alvo),pasta_alvo)