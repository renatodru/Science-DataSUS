import mysql.connector
from mysql.connector import errorcode
import os
from os import listdir
from dbf import Table

def conecta_db():
    config = {'user':'root','password':'root','host':'localhost','port':'3306','raise_on_warnings':True}
    try:cnx = mysql.connector.connect(**config)#user='root',password='root',host='127.0.0.1',port='3306',raise_on_warnings=True
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:print("Senha ou usuario incorretos")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:print("Database não existe")
        else:print(err)
    else:print("Conectado em {}:{}, usuario: {}".format(config['host'],config['port'],config['user']))
    return cnx

def lista_alvos():
    dict_pasta_arquivo = {}
    pasta_atual = os.path.dirname(__file__)+"\\"
    subpastas_nome = [f.name for f in os.scandir(pasta_atual) if f.is_dir()]
    
    for subpasta in subpastas_nome:
        lista_arq = [arq for arq in listdir(pasta_atual+subpasta) if arq[-3:]=="dbf"]
        if len(lista_arq)>0:dict_pasta_arquivo.update({subpasta:lista_arq})
        
    historico = [lista_h[:-1] for lista_h in open(pasta_atual+"DBF_importados_MySQL.txt",'r').readlines()]
    excluir = []
    for x in dict_pasta_arquivo:
        for h in historico:
            if h in dict_pasta_arquivo[x]:dict_pasta_arquivo[x].remove(h)
        if(len(dict_pasta_arquivo[x])==0):excluir.append(x)    
    if(len(excluir)>0):
        for exc in excluir: del dict_pasta_arquivo[exc]    
    return dict_pasta_arquivo

def abre_dbf(subpasta,dbf_alvo):#devolve o arquivo como um objeto table do dbf
    localatual = os.path.dirname(__file__)+str('\\')
    dbf_file = localatual+subpasta+"\\"+dbf_alvo
    return Table(dbf_file).open() #atribui o DBF à variavel, a abre e retorna como chamado da função
#    placenames.open()#metodo que abre a tabela
#    print(placenames)#imprime o cabeçalho e informações gerais do banco
#    print(placenames[0])#imprime a primeira linha
#print(abre_dbf("RDPR","RDPR2101.dbf")) #chama a função e imprime a primeira linha
#print(abre_dbf()[0][0])#imprime a primeira coluna da primeira linha

def escreve_bd(cnx,dict_alvos):
    try:cursor = cnx.cursor()
    except:print("Erro definindo cursor")
    database = 'DATASUS'
    cursor.execute("USE {}".format(database))
    print("Alvos para importação {}".format(dict_alvos))

    for pasta in dict_alvos:
        table = pasta
        for arq in dict_alvos[pasta]:
            dbf_file = abre_dbf(pasta,arq)
            print("{} carregado".format(arq), end='')
            #cursor.fast_execute = True
            qnt_values = len(tuple(dbf_file.field_names))
            print("Cabeçalho de {} carregado".format(arq), end='')
            cursor.execute('SET GLOBAL max_allowed_packet=500*1024*1024')
            cursor.execute('SET GLOBAL wait_timeout = 28800')
            
            data = [tuple(row) for row in dbf_file]
            print("Linhas de {} carregado".format(arq), end='')
            stmt = "INSERT INTO {} {} VALUES ({} {})".format(pasta, str(tuple(dbf_file.field_names)).replace("'", ""), "%s, "*(qnt_values-1), "%s" )
            print("Query montada", end='')
            cursor.executemany(stmt, data)
            print("Query executada", end='')
            cnx.commit()
            print("Commit executado")
    cursor.close()
    cnx.close()


print(lista_alvos())

#lista_alv = {"RDPR":["RDPR2102.dbf"]} #dbf para teste
escreve_bd(conecta_db(),lista_alvos())