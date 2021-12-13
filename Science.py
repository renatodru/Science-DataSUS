import mysql.connector
from mysql.connector import errorcode
import os
from os import listdir
import pandas as pd

def conecta_db():
    config = {'user':'root','password':'root','host':'localhost','port':'3306','raise_on_warnings':True}
    try:cnx = mysql.connector.connect(**config)#user='root',password='root',host='127.0.0.1',port='3306',raise_on_warnings=True
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:print("Senha ou usuario incorretos")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:print("Database não existe")
        else:print(err)
    else:print("Conectado em {}:{}, usuario: {}".format(config['host'],config['port'],config['user']))
    return cnx

def table_mysql(query,cnx):
    try:cursor = cnx.cursor()
    except:print("Erro definindo cursor");return
    
    database = 'DATASUS'
    cursor.execute("USE {}".format(database))
    cursor.execute('SELECT * FROM {}'.format(query))

    return cursor.fetchall()

cnx = conecta_db()

df_RDPR = pd.DataFrame(table_mysql("RDPR WHERE CNES = '2384299'",cnx))
df_SPPR = pd.DataFrame(table_mysql("SPPR WHERE SP_CNES = '2384299'",cnx))




cursor.close()
cnx.close()
