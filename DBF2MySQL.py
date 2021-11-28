import mysql.connector
from mysql.connector import errorcode
import os
from os import listdir
from dbf import Table

def conecta_db():
    config = {'user':'root','password':'root','host':'127.0.0.1','port':'3306','raise_on_warnings':True}
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

print(type(abre_dbf("PAPR","RDPR2101.dbf")[0])) #chama a função e imprime a primeira linha
#print(abre_dbf()[0][0])#imprime a primeira coluna da primeira linha

def escreve_bd(cnx,dict_alvos):
    try:cursor = cnx.cursor()
    except:print("erro definindo cursor")
    database = 'DATASUS'
    cursor.execute("USE {}".format(database))
    contador = 0
    
    for pasta in dict_alvos:
        table = pasta
        for arq in dict_alvos[pasta]:
            dbf_file = abre_dbf(pasta,arq)
            for linha_dbf in dbf_file:
                contador += 1
                if contador == 10: return True
                
                pass
            
            
    cursor.close()
            


#    for linha in abre_dbf():#chama a tabela linha a linha
#        query = """INSERT mytb SET column1 = %s, column2 = %s, column3 = %s"""
#        values = (linha["column1"], linha["column2"], linha["column3"])
#        cur.execute(query, (linha["column1"], linha["column2"], linha["column3"]))
#        print(linha["column1"], linha["column2"], linha["column3"])
    
#escreve_bd(conecta_db(),lista_alvos())

#cnx.close()