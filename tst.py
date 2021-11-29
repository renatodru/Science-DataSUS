import mysql.connector
from mysql.connector import errorcode
import os
from os import listdir
from dbf import Table

config = {'user':'root','password':'root','host':'127.0.0.1','port':'3306','raise_on_warnings':True}
try:cnx = mysql.connector.connect(**config)#user='root',password='root',host='127.0.0.1',port='3306',raise_on_warnings=True
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:print("Senha ou usuario incorretos")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:print("Database não existe")
    else:print(err)
else:print("Conectado em {}:{}, usuario: {}".format(config['host'],config['port'],config['user']))


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
    except:print("erro definindo cursor")
    database = 'DATASUS'
    cursor.execute("USE {}".format(database))
    contador = 0

    for pasta in dict_alvos:
        table = pasta
        for arq in dict_alvos[pasta]:
            dbf_file = abre_dbf(pasta,arq)
            #print(dbf_file)
            #print(tuple(dbf_file.field_names),'\n\n')
            #print(tuple(dbf_file[0]))
            #cursor.fast_execute = True
            qnt_values = len(tuple(dbf_file.field_names))
            data = [tuple(row) for row in dbf_file]
            stmt = "INSERT INTO "+pasta+" "+ str(tuple(dbf_file.field_names)).replace("'", "") +" VALUES ("+"%s, "*(qnt_values-1) +"%s)"
            #print(data,"\n\n")
            #print(stmt)
            cursor.executemany(stmt, data)
            #cursor.executemany("INSERT INTO {0} {1} VALUES ({2})".format(table, tuple(dbf_file.field_names),dbf_file))#,multi=True)
            cnx.commit()
    cursor.close()

def sql_insert(table_name, fields, rows, truncate_table = True):
    if len(rows) == 0:return

    cursor = mdwh_connection.cursor()
    cursor.fast_executemany = True
    values_sql = ('?, ' * (fields.count(',') + 1))[:-2]

    if truncate_table: sql_truncate(table_name, cursor)    
    insert_sql = 'insert {0} ({1}) values ({2});'.format(table_name, fields, values_sql)
    current_row = 0
    batch_size = 50000

    while current_row < len(rows):
        cursor.executemany(insert_sql, rows[current_row:current_row + batch_size])
        mdwh_connection.commit()
        current_row += batch_size
        logging.info(
            '{} more records inserted. Total: {}'.format(
                min(batch_size,len(rows)-current_row+batch_size),
                min(current_row, len(rows))
            )
        )   



lista_alv = {"RDPR":["RDPR2101.dbf"]}            
escreve_bd(cnx,lista_alv)

#for linha in abre_dbf():#chama a tabela linha a linha
#    query = """INSERT mytb SET column1 = %s, column2 = %s, column3 = %s"""
#    values = (linha["column1"], linha["column2"], linha["column3"])
#    print(linha["column1"], linha["column2"], linha["column3"])

#cnx.close()